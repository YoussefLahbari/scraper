
const mql = window.matchMedia('(prefers-color-scheme: dark)')

function setDarkMode() {
    if(mql.matches) {
        document.body.classList.add("dark");
        document.body.classList.remove("light");
    } else {
        document.body.classList.remove("dark");
        document.body.classList.add("light");
    }
}

setDarkMode()

mql.addEventListener("change", setDarkMode);