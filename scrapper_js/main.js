const {app, BrowserWindow, ipcMain, Notification, shell} = require('electron');
const fs = require("fs");
const iconv = require('iconv-lite');
const fetch = require("node-fetch");
const jsdom = require("jsdom");
const child_process = require("child_process");
const path = require("path");

// Define states for scraping (matching Python script)
const STATES = [
    "Baden-W%FCrttemberg",  // Baden-Württemberg properly encoded
    "Bayern",
    "Berlin",
    "Brandenburg",
    "Bremen",
    "Hamburg",
    "Hessen",
    "Mecklenburg-Vorpommern",
    "Niedersachsen",
    "Nordrhein-Westfalen",
    "Rheinland-Pfalz",
    "Saarland",
    "Sachsen",
    "Sachsen-Anhalt",
    "Schleswig-Holstein",
    "Th%FCringen"  // Thüringen properly encoded
];

// Display names for nicer UI
const STATE_DISPLAY_NAMES = {
    "Baden-W%FCrttemberg": "Baden-Württemberg",
    "Bayern": "Bayern",
    "Berlin": "Berlin",
    "Brandenburg": "Brandenburg",
    "Bremen": "Bremen",
    "Hamburg": "Hamburg",
    "Hessen": "Hessen",
    "Mecklenburg-Vorpommern": "Mecklenburg-Vorpommern",
    "Niedersachsen": "Niedersachsen",
    "Nordrhein-Westfalen": "Nordrhein-Westfalen",
    "Rheinland-Pfalz": "Rheinland-Pfalz",
    "Saarland": "Saarland",
    "Sachsen": "Sachsen",
    "Sachsen-Anhalt": "Sachsen-Anhalt",
    "Schleswig-Holstein": "Schleswig-Holstein",
    "Th%FCringen": "Thüringen"
};

// Extended list of CSV keys to include more data
const CSVKeys = [
    "Firmenname", "Adresse", "PLZ", "Ort", "Branche", 
    "E-Mail", "Homepage", "Telefon", "Fax", "ImageURL", 
    "TYPO3", "Shopware"
];

// Random user agents to avoid blocking
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0',
];

let sender = null;
let stop = false;
let running = false;
let currentState = null;
let loadedPages = 0;
let total = 0;
let processedCount = 0;
let frParams = {}; // Cache for fr_params by state

// Add proxy configuration
const ProxyManager = require('./proxy_manager');
const proxyManager = new ProxyManager();
let currentProxy = null;
let consecutiveFailures = 0;
const MAX_FAILURES_BEFORE_ROTATE = 2;

// Add more sophisticated rate limiting to avoid blocks
const rateLimit = {
    lastRequest: Date.now(),
    minDelay: 1000,  // Minimum delay between requests in ms
    applyDelay: function() {
        const now = Date.now();
        const elapsed = now - this.lastRequest;
        
        // If we've made a request too recently, delay the next one
        if (elapsed < this.minDelay) {
            return new Promise(resolve => {
                const delay = this.minDelay - elapsed;
                setTimeout(resolve, delay);
            });
        }
        return Promise.resolve();
    },
    recordRequest: function() {
        this.lastRequest = Date.now();
    }
};

function createWindow() {
    // Initialize the proxy manager
    proxyManager.initialize().then(() => {
        console.log(`Proxy manager initialized with ${proxyManager.getProxyCount()} proxies`);
    }).catch(error => {
        console.error(`Error initializing proxy manager: ${error.message}`);
    });

    let win = new BrowserWindow({
        width: 1000,
        height: 800,
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false
        }
    });

    win.setMenu(null);
    win.loadFile("page/index.html");

    // Initialize the UI by sending states list
    win.webContents.on('did-finish-load', () => {
        win.webContents.send("states", STATES.map(state => ({
            code: state,
            name: STATE_DISPLAY_NAMES[state] || state
        })));
    });

    ipcMain.on('getStates', (e) => {
        e.sender.send("states", STATES.map(state => ({
            code: state,
            name: STATE_DISPLAY_NAMES[state] || state
        })));
    });

    ipcMain.on('downloadByState', (e, state) => {
        if (running) {
            return;
        }
        running = true;
        stop = false;
        currentState = state;
        sender = e.sender;
        loadedPages = 0;
        total = 0;
        processedCount = 0;
        
        sender.send("message", `Starting download for ${STATE_DISPLAY_NAMES[state]}`);
        console.log(`Starting download for state: ${STATE_DISPLAY_NAMES[state]}`);
        
        // Start with the first page for the selected state
        const url = `http://www.firmenregister.de/register.php?cmd=search&stichwort=&firma=&branche=&vonplz=&ort=&strasse=&vorwahl=&bundesland=${state}&Suchen=Suchen`;
        
        fetchWithRetries(url, 3)
            .then(res => run(res, state))
            .catch((err) => {
                running = false;
                new Notification({
                    title: "Firmen Scraper",
                    urgency: "critical",
                    body: err.stack && err.stack.toString && err.stack.toString()
                }).show();
            });
    });

    ipcMain.on('save', (e) => {
        stop = true;
        updateCounter();
    });

    // Add the handler for opening the folder (missing from previous code)
    ipcMain.on('openFolder', () => {
        shell.openPath(path.join(__dirname, "csv"));
    });

    // Add new IPC handler for updating proxies
    ipcMain.on('refreshProxies', (e) => {
        proxyManager.refreshProxies().then(() => {
            e.sender.send("proxyStatus", {
                count: proxyManager.getProxyCount(),
                message: `Refreshed proxy list, found ${proxyManager.getProxyCount()} working proxies`
            });
        }).catch(error => {
            e.sender.send("proxyStatus", {
                count: proxyManager.getProxyCount(),
                message: `Error refreshing proxies: ${error.message}`
            });
        });
    });

    ipcMain.on('getProxyStatus', (e) => {
        e.sender.send("proxyStatus", {
            count: proxyManager.getProxyCount(),
            message: `${proxyManager.getProxyCount()} proxies available`
        });
    });
}

// Fetch with retries and random delays
function fetchWithRetries(url, maxRetries = 3, retryCount = 0) {
    // Apply rate limiting
    return rateLimit.applyDelay().then(() => {
        const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
        const headers = {
            'User-Agent': userAgent,
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Referer': 'https://firmenregister.de/',
            'Cache-Control': 'no-cache'
        };
        
        // Get a proxy if we don't have one or it's time to rotate
        if (!currentProxy || consecutiveFailures >= MAX_FAILURES_BEFORE_ROTATE) {
            currentProxy = proxyManager.getNextProxy();
            console.log(`Using proxy: ${currentProxy ? currentProxy.ip : 'Direct connection'}`);
            consecutiveFailures = 0;
        }
        
        console.log(`Fetching: ${url} (attempt ${retryCount + 1}/${maxRetries})`);
        
        // Configure fetch options with proxy if available
        const fetchOptions = { 
            headers,
            timeout: 30000 // 30 seconds timeout
        };
        
        // Add proxy if available
        if (currentProxy && currentProxy.ip && currentProxy.port) {
            const proxyUrl = `http://${currentProxy.ip}:${currentProxy.port}`;
            fetchOptions.agent = new require('https-proxy-agent')(proxyUrl);
            console.log(`Using proxy agent: ${proxyUrl}`);
        }
        
        // Record this request with the rate limiter
        rateLimit.recordRequest();
        
        return fetch(url, fetchOptions)
            .then(res => {
                if (res.status === 200) {
                    // Reset failure counter on success
                    consecutiveFailures = 0;
                    return res;
                } else if (res.status === 403 || res.status === 429) {
                    console.log(`Request blocked with status ${res.status}. Rotating proxy and waiting before retry...`);
                    
                    // Increment failures and rotate proxy on next attempt
                    consecutiveFailures++;
                    
                    return new Promise((resolve) => {
                        const waitTime = 2000 + Math.random() * 3000; // 2-5 seconds
                        setTimeout(() => {
                            if (retryCount < maxRetries) {
                                // Force proxy rotation
                                currentProxy = proxyManager.getNextProxy();
                                console.log(`Rotated to new proxy: ${currentProxy ? currentProxy.ip : 'Direct connection'}`);
                                
                                resolve(fetchWithRetries(url, maxRetries, retryCount + 1));
                            } else {
                                console.log(`Failed after ${maxRetries} attempts`);
                                throw new Error(`Request failed with status ${res.status}`);
                            }
                        }, waitTime);
                    });
                } else {
                    // Increment failures for other errors
                    consecutiveFailures++;
                    throw new Error(`Request failed with status ${res.status}`);
                }
            })
            .catch(error => {
                // Increment failures on network errors
                consecutiveFailures++;
                
                if (retryCount < maxRetries) {
                    const waitTime = 2000 + Math.random() * 3000;
                    console.log(`Request error: ${error.message}. Waiting ${waitTime}ms before retrying...`);
                    
                    return new Promise(resolve => {
                        setTimeout(() => {
                            // Rotate proxy if we've had multiple failures
                            if (consecutiveFailures >= MAX_FAILURES_BEFORE_ROTATE) {
                                currentProxy = proxyManager.getNextProxy();
                                console.log(`Rotated to new proxy after error: ${currentProxy ? currentProxy.ip : 'Direct connection'}`);
                                consecutiveFailures = 0;
                            }
                            
                            resolve(fetchWithRetries(url, maxRetries, retryCount + 1));
                        }, waitTime);
                    });
                }
                
                throw error;
            });
    });
}

function run(res, state) {
    prepare(res, true)
        .then(([pages, pageurl, csv]) => {
            loadedPages = 1;
            total = pages;
            updateCounter();
            
            // Cache the fr_param for this state
            if (pageurl) {
                const frMatch = pageurl.match(/fr=([^&]+)/);
                if (frMatch && frMatch[1]) {
                    frParams[state] = frMatch[1];
                    console.log(`Cached fr_param for ${state}: ${frParams[state]}`);
                }
            }
            
            let promises = [];
            // Process one page at a time to avoid getting banned
            processNextPage(pageurl, 1, pages, csv, state);
        });
}

function processNextPage(pageurl, page, totalPages, collectedData, state) {
    if (stop || page >= totalPages) {
        finalizeScraping(collectedData, state);
        return;
    }
    
    sender.send("message", `Processing page ${page + 1} of ${totalPages}`);
    
    // Add random delay between page requests (1-3 seconds)
    setTimeout(() => {
        getPage(pageurl, page)
            .then(pageData => {
                if (pageData && Array.isArray(pageData)) {
                    collectedData = collectedData.concat(pageData);
                    processedCount += pageData.length;
                    loadedPages++;
                    updateCounter();
                    
                    // Process the next page
                    processNextPage(pageurl, page + 1, totalPages, collectedData, state);
                } else {
                    console.log(`No data returned for page ${page + 1}. Finalizing.`);
                    finalizeScraping(collectedData, state);
                }
            })
            .catch(err => {
                console.error(`Error processing page ${page + 1}:`, err);
                finalizeScraping(collectedData, state);
            });
    }, 1000 + Math.random() * 2000);
}

function finalizeScraping(collectedData, state) {
    Promise.all(collectedData.map(findCMS))
        .then(arr => arr.map(objectToArray))
        .then(arr => {
            arr.unshift(CSVKeys);
            return arr;
        })
        .then(csvObjectToString)
        .then(data => wrapFsWrite(data, state))
        .then((path) => {
            running = false;
            let notification = new Notification({
                title: "Firmen Scraper",
                body: `Successfully saved ${processedCount} companies to ${path}`,
                urgency: "normal"
            });
            notification.show();
            shell.openPath(__dirname + "/csv");
        });
}

function updateCounter() {
    if (stop) {
        sender.send('message', 'Stopping...');
    } else {
        sender.send("message", `Processed ${processedCount} companies from ${loadedPages}/${total} pages`);
        sender.send("progress", { processed: processedCount, pages: { current: loadedPages, total } });
    }
}

function getPage(link, page, retry = 0) {
    if (stop) {
        return Promise.resolve(undefined);
    }
    
    console.log(`Loading page: ${page + 1} Retry: ${retry}`);
    
    // Construct URL for the specific page
    let pageUrl;
    if (page === 0) {
        // First page
        pageUrl = link;
    } else {
        // Subsequent pages using fr parameter
        if (currentState && frParams[currentState]) {
            pageUrl = `http://www.firmenregister.de/register.php?cmd=mysearch&fr=${frParams[currentState]}&auswahl=alle&ap=${page}`;
        } else {
            pageUrl = `${link}&ap=${page}`;
        }
    }
    
    return new Promise((resolve, reject) => {
        fetchWithRetries(pageUrl, 3)
            .then(res => prepare(res, false, retry))
            .then(resolve)
            .catch(err => {
                console.error(`Error fetching page ${page + 1}:`, err);
                if (retry < 2) {
                    // Add increasing delay before retry
                    setTimeout(() => {
                        resolve(getPage(link, page, retry + 1));
                    }, 3000 + (retry * 2000));
                } else {
                    resolve(undefined);
                }
            });
    });
}

function prepare(res, requestPage = false, retry = 0) {
    if (res === undefined) {
        return undefined;
    }
    return res.text()
        .then(html => new jsdom.JSDOM(html, "text/html"))
        .then(dom => dom.window.document)
        .then(dom => buildCSV(dom, requestPage));
}

function buildCSV(dom, requestPage) {
    const content = dom.getElementById("content");
    if (!content) {
        console.error("Could not find content element");
        return requestPage ? [0, "", []] : [];
    }
    
    const tables = content.getElementsByTagName("table");
    if (!tables || tables.length === 0) {
        console.error("No tables found in content");
        return requestPage ? [0, "", []] : [];
    }
    
    const table = tables[tables.length - 1];
    const rows = table.querySelector("tbody").children;
    
    let pages = 0;
    let pageUrl = "";
    let csv = [];
    
    for (let row of rows) {
        if (row.childElementCount !== 4) {
            // This might be a pagination row
            if (row.textContent.includes("Seiten:") && requestPage) {
                const anchors = row.getElementsByTagName("a");
                const values = Array.from(anchors)
                    .map(a => a.textContent)
                    .map(text => parseInt(text))
                    .map(num => isNaN(num) ? 0 : num);
                
                if (anchors.length > 1) {
                    pageUrl = anchors[1].href.replace(/&ap=\d+/, "");
                    pages = Math.max(...values);
                    console.log(`Found ${pages} pages. Page URL: ${pageUrl}`);
                }
            }
            continue;
        }
        
        const childElements = row.children;
        if (childElements[0].textContent === "Info") {
            continue;
        }
        
        // Extract company data
        const current = {};
        
        // Extract info from first column (email, website, products)
        const infoField = childElements[0].getElementsByTagName("a");
        for (let anchor of infoField) {
            if (anchor.href.startsWith("mailto:")) {
                current["E-Mail"] = anchor.href.replace("mailto:", "");
            } else if (anchor.href.startsWith("click")) {
                current["Homepage"] = anchor.getAttribute("onmouseover").replace("return escape('", "").replace("')", "");
            }
        }
        
        // Look for product info
        const productInfo = childElements[0].querySelector("img[src=\"pic/prod.gif\"]");
        if (productInfo && productInfo.getAttribute("onmouseover")) {
            const productDesc = productInfo.getAttribute("onmouseover");
            const productMatch = productDesc.match(/return escape\('([^']+)'\)/);
            if (productMatch) {
                current["Produkte"] = productMatch[1];
            }
        }
        
        // Extract company name, address, postal code from second column
        const addressAnchors = childElements[1].getElementsByTagName("a");
        if (addressAnchors.length > 0) {
            current["Firmenname"] = addressAnchors[0].textContent.trim();
            
            // Extract address if available
            if (addressAnchors.length > 1) {
                current["Adresse"] = addressAnchors[1].textContent.trim();
            }
            
            // Extract postal code and city
            if (addressAnchors.length > 2) {
                current["PLZ"] = addressAnchors[2].textContent.trim();
                
                if (addressAnchors.length > 3) {
                    current["Ort"] = addressAnchors[3].textContent.trim();
                }
            }
        }
        
        // Extract industry/branch information from third column
        current["Branche"] = childElements[2].textContent.trim();
        
        // Extract image URL from fourth column
        const imageElement = childElements[3].querySelector("img");
        if (imageElement && imageElement.src) {
            // Extract the image URL and convert to absolute URL if needed
            let imageUrl = imageElement.src;
            if (!imageUrl.startsWith("http")) {
                // Convert to absolute URL
                imageUrl = new URL(imageUrl, "http://www.firmenregister.de/").href;
            }
            current["ImageURL"] = imageUrl;
        }
        
        csv.push(current);
    }
    
    if (loadedPages) {
        loadedPages++;
        updateCounter();
    }
    
    return requestPage ? [pages, pageUrl, csv] : csv;
}

// Modify the findCMS function to also handle image extraction if not already present
function findCMS(dataset) {
    let homepage = dataset["Homepage"];

    if (!homepage) {
        return Promise.resolve(dataset);
    }

    return new Promise((resolve) => {
        fetchCatch(homepage)
            .then(res => res.text())
            .then(html => {
                if (html.includes("typo3")) {
                    dataset["TYPO3"] = "wahrscheinlich";
                    resolve(dataset);
                } else {
                    fetchCatch(homepage + "/backend")
                        .then(res => res.text())
                        .then(html1 => {
                            if (html1.includes("shopware")) {
                                dataset["Shopware"] = "wahrscheinlich";
                                resolve(dataset);
                            } else {
                                fetchCatch(homepage + "/admin")
                                    .then(res => res.text())
                                    .then(html2 => {
                                        if (html2.includes("wahrscheinlich shopware")) {
                                            dataset["Shopware"] = "wahrscheinlich";
                                            resolve(dataset);
                                        } else if (html.includes("sw-")) {
                                            dataset["Shopware"] = "möglicherweise";
                                            resolve(dataset);
                                        } else {
                                            let errorOnTypo3 = html === "error";
                                            let errorOnShopware = html1 === "error" || html2 === "error";
                                            let fullErrorOnShopware = html1 === "error" && html2 === "error";
                                            dataset["TYPO3"] = html === "error" ? "error" : "wahrscheinlich nicht"
                                            dataset["Shopware"] = fullErrorOnShopware ? "error" : errorOnShopware ? "möglicherweise nicht" : "wahrscheinlich nicht"
                                            resolve(dataset);
                                        }
                                    });
                            }
                        });
                }
            })
    })
}

function fetchCatch(url) {
    return new Promise(resolve => {
        fetch(url)
            .then(resolve)
            .catch(ignored => resolve({
                text() {
                    return "error";
                }
            }))
    })
}

function objectToArray(object) {
    let out = [];
    for (let x = 0; x < CSVKeys.length; x++) {
        out.push("");
    }
    for (let x in object) {
        const index = CSVKeys.indexOf(x);
        if (index !== -1) {
            out[index] = object[x];
        }
    }
    return out;
}

function wrapFsWrite(data, state) {
    return new Promise(((resolve, reject) => {
        let date = new Date();
        let dir = __dirname + "/csv/";
        
        // Create a filename based on state
        const stateName = STATE_DISPLAY_NAMES[state].replace(/ /g, "_").toLowerCase();
        let filepath = dir + `firmenregister_${stateName}_${date.getFullYear()}_${date.getMonth() + 1}_${date.getDate()}.csv`;
        
        fs.mkdir(dir, { recursive: true }, (err) => {
            if (err && err.code !== "EEXIST") {
                reject(err);
            }

            fs.writeFile(filepath, "\uFEFF" + data, ((err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(filepath)
                }
            }))
        });
    }));
}

function csvObjectToString(csvObject) {
    return csvObject.map((value => value.join(";"))).join("\n")
}

app.whenReady().then(createWindow);
