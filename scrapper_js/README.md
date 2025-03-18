# Firmenregister.de Scraper

This application scrapes company data from firmenregister.de by state (Bundesland), with built-in IP rotation and rate limiting to avoid blocking.

## Features

- Scrape by state (Bundesland)
- Smart proxy rotation
- Rate limiting and request throttling
- Progress tracking and resumable scraping
- Detailed statistics
- Detects CMS types (TYPO3, Shopware)
- Extracts company logos/images
- Clean and intuitive UI
- Automatic CSV export

## Installation

1. Install Node.js (version 14 or higher recommended)
2. Clone this repository
3. Run `npm install` to install dependencies
4. Start the application with `npm start`

## Usage

1. Launch the application
2. Select a state (Bundesland) from the dropdown menu
3. Click "Start Scraping"
4. Progress will be displayed in real-time
5. When completed, CSV files will be saved in the `csv` directory

## Proxy Configuration

The application automatically fetches and tests free proxies. You can also:

- Click "Refresh Proxies" to manually refresh the proxy list
- Edit `proxy_manager.js` to add your own proxy sources
- Disable proxies by setting `useProxies: false` in the `ProxyManager` constructor

## Output Format

The output CSV files contain the following columns:

- Firmenname: Company name
- Adresse: Street address
- PLZ: Postal code
- Ort: City
- Branche: Industry sector
- E-Mail: Email address
- Homepage: Website URL
- Telefon: Phone number
- Fax: Fax number
- ImageURL: URL to the company logo
- TYPO3: Indicates if the company website uses TYPO3 CMS
- Shopware: Indicates if the company website uses Shopware

## Troubleshooting

If the application is blocked or encounters errors:

1. Try refreshing the proxy list
2. Check the logs directory for detailed error information
3. Increase the rate limiting delay in main.js
4. Restart the application to resume from the last saved point

## Building Standalone Application

To build a standalone application:

```
npm run dist
```

This will create an executable in the `dist` directory.

## License

This software is for educational purposes only. Use responsibly and in accordance with the terms of service of firmenregister.de.
