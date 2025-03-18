const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const https = require('https');

class ProxyManager {
    constructor() {
        this.proxies = [];
        this.currentIndex = 0;
        this.proxyFileName = path.join(__dirname, 'working_proxies.json');
        this.useProxies = true; // Set to false to disable proxy usage
    }
    
    async initialize() {
        // Try to load saved proxies first
        await this.loadSavedProxies();
        
        // If we don't have enough proxies, refresh the list
        if (this.proxies.length < 5) {
            await this.refreshProxies();
        }
        
        return this.proxies.length;
    }
    
    async loadSavedProxies() {
        try {
            if (fs.existsSync(this.proxyFileName)) {
                const data = fs.readFileSync(this.proxyFileName, 'utf8');
                const savedProxies = JSON.parse(data);
                
                // Filter proxies based on their last check time (discard if older than 24 hours)
                const currentTime = new Date().getTime();
                const validProxies = savedProxies.filter(proxy => {
                    if (!proxy.lastChecked) return false;
                    const lastChecked = new Date(proxy.lastChecked).getTime();
                    const hoursSinceLastCheck = (currentTime - lastChecked) / (1000 * 60 * 60);
                    return hoursSinceLastCheck < 24;
                });
                
                this.proxies = validProxies;
                console.log(`Loaded ${this.proxies.length} valid proxies from file`);
            }
        } catch (error) {
            console.error('Error loading saved proxies:', error);
            this.proxies = [];
        }
    }
    
    saveSavedProxies() {
        try {
            fs.writeFileSync(this.proxyFileName, JSON.stringify(this.proxies, null, 2));
            console.log(`Saved ${this.proxies.length} proxies to file`);
        } catch (error) {
            console.error('Error saving proxies:', error);
        }
    }
    
    async refreshProxies() {
        try {
            const newProxies = await this.fetchFreeProxies();
            console.log(`Fetched ${newProxies.length} new proxies, testing them...`);
            
            const validProxies = await this.testProxies(newProxies);
            console.log(`Found ${validProxies.length} working proxies`);
            
            // Update the proxies list
            this.proxies = validProxies;
            this.currentIndex = 0;
            
            // Save the working proxies
            this.saveSavedProxies();
            
            return this.proxies.length;
        } catch (error) {
            console.error('Error refreshing proxies:', error);
            throw error;
        }
    }
    
    async fetchFreeProxies() {
        const proxies = [];
        
        try {
            // Get proxies from free-proxy-list.net
            const response = await fetch('https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt');
            const text = await response.text();
            
            // Parse the text into proxy objects
            const lines = text.split('\n');
            for (const line of lines) {
                const trimmed = line.trim();
                if (trimmed) {
                    const [ip, port] = trimmed.split(':');
                    if (ip && port) {
                        proxies.push({
                            ip,
                            port: parseInt(port),
                            protocol: 'http',
                            lastChecked: null,
                            workingForSite: null
                        });
                    }
                }
            }
            
            // If that source didn't work, we can add more sources here
            if (proxies.length < 10) {
                // Try another source like geonode
                const geonodeResponse = await fetch('https://proxylist.geonode.com/api/proxy-list?limit=50&page=1&sort_by=lastChecked&sort_type=desc&filterUpTime=90&protocols=http%2Chttps');
                const geonodeData = await geonodeResponse.json();
                
                if (geonodeData && geonodeData.data) {
                    for (const proxy of geonodeData.data) {
                        proxies.push({
                            ip: proxy.ip,
                            port: parseInt(proxy.port),
                            protocol: proxy.protocols[0],
                            lastChecked: null,
                            workingForSite: null
                        });
                    }
                }
            }
        } catch (error) {
            console.error('Error fetching free proxies:', error);
        }
        
        return proxies;
    }
    
    async testProxies(proxiesToTest) {
        const workingProxies = [];
        const testUrl = 'http://www.firmenregister.de/'; // URL to test against
        
        // Create an agent that ignores SSL errors for testing
        const httpsAgent = new https.Agent({
            rejectUnauthorized: false
        });
        
        for (const proxy of proxiesToTest) {
            try {
                const proxyUrl = `${proxy.protocol}://${proxy.ip}:${proxy.port}`;
                const proxyAgent = require('https-proxy-agent')(proxyUrl);
                
                // Try to fetch with this proxy
                const response = await fetch(testUrl, {
                    agent: proxyAgent,
                    timeout: 10000, // 10 second timeout
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                });
                
                if (response.status === 200) {
                    // Check if we got actual content
                    const text = await response.text();
                    if (text.includes('firmenregister') || text.includes('Firmenregister')) {
                        proxy.lastChecked = new Date().toISOString();
                        proxy.workingForSite = true;
                        workingProxies.push(proxy);
                        console.log(`Proxy ${proxy.ip}:${proxy.port} is working`);
                    } else {
                        console.log(`Proxy ${proxy.ip}:${proxy.port} returned status 200 but unexpected content`);
                    }
                } else {
                    console.log(`Proxy ${proxy.ip}:${proxy.port} returned status ${response.status}`);
                }
            } catch (error) {
                console.log(`Proxy ${proxy.ip}:${proxy.port} failed: ${error.message}`);
            }
        }
        
        return workingProxies;
    }
    
    // Add a method to test a specific proxy
    async testProxy(proxy) {
        const testUrl = 'http://www.firmenregister.de/';
        try {
            const proxyUrl = `${proxy.protocol}://${proxy.ip}:${proxy.port}`;
            const proxyAgent = require('https-proxy-agent')(proxyUrl);
            
            // Try to fetch with this proxy
            const response = await fetch(testUrl, {
                agent: proxyAgent,
                timeout: 8000, // 8 second timeout
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            });
            
            if (response.status === 200) {
                // Validate actual content
                const text = await response.text();
                return text.includes('firmenregister') || text.includes('Firmenregister');
            }
            
            return false;
        } catch (error) {
            console.log(`Proxy test failed: ${error.message}`);
            return false;
        }
    }

    getNextProxy() {
        if (!this.useProxies || this.proxies.length === 0) {
            return null; // No proxies available, use direct connection
        }
        
        if (this.currentIndex >= this.proxies.length) {
            this.currentIndex = 0; // Reset to start if we've used all proxies
        }
        
        const proxy = this.proxies[this.currentIndex];
        this.currentIndex++;
        
        return proxy;
    }
    
    getProxyCount() {
        return this.proxies.length;
    }
    
    markProxyAsFailed(proxy) {
        if (!proxy) return;
        
        // Mark this proxy as failed and move it to the end of the list
        const index = this.proxies.findIndex(p => p.ip === proxy.ip && p.port === proxy.port);
        if (index !== -1) {
            this.proxies.splice(index, 1); // Remove from list
            
            // We could choose to discard completely or move to end of list
            // In this case, we'll discard completely
            
            // Update the saved file
            this.saveSavedProxies();
        }
    }

    // Add a method to handle proxy rotation on failure
    async rotateOnFailure() {
        // Get the next proxy
        const proxy = this.getNextProxy();
        
        // If we're running low on proxies, fetch more in background
        if (this.currentIndex >= this.proxies.length - 3) {
            this.refreshProxies().catch(error => 
                console.error('Background proxy refresh failed:', error));
        }
        
        return proxy;
    }
}

module.exports = ProxyManager;
