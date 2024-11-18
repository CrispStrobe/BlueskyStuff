const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// Configuration
const OUTPUT_FILE = 'starter_pack_urls.txt';
const FAILED_PAGES_FILE = 'failed_pages.log';
const CONCURRENT_PAGES = 10; // Number of pages to process concurrently
const MAX_RETRIES = 3; // Maximum number of retries per page
const DELAY_BETWEEN_BATCHES = 100; // Delay in ms between processing batches

// Initialize write streams
const urlStream = fs.createWriteStream(OUTPUT_FILE, { flags: 'a' }); // Append mode
const failedPagesStream = fs.createWriteStream(FAILED_PAGES_FILE, { flags: 'a' }); // Append mode

/**
 * Delay helper
 * @param {number} ms - Milliseconds to delay
 */
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Process a single page with retry mechanism
 * @param {puppeteer.Browser} browser - Puppeteer browser instance
 * @param {number} pageNum - Page number to scrape
 * @param {number} retries - Current retry count
 * @returns {Promise<number>} - Number of packs found on the page
 */
async function processPage(browser, pageNum, retries = 0) {
    const page = await browser.newPage();

    try {
        // Optimize performance by blocking unnecessary resources
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            const resourceType = req.resourceType();
            if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
                req.abort();
            } else {
                req.continue();
            }
        });

        console.log(`Scraping page ${pageNum} (Attempt ${retries + 1}/${MAX_RETRIES})`);
        await page.goto(`https://blueskydirectory.com/starter-packs/all?page=${pageNum}`, {
            waitUntil: 'networkidle2',
            timeout: 60000, // 60 seconds timeout
        });

        // Wait for the paginator buttons to load
        await page.waitForSelector('button[wire\\:key^="paginator-page-page"]', { timeout: 30000 });

        // Extract starter pack URLs in 'creatorHandle|rkey' format
        const pagePacks = await page.evaluate(() => {
            const buttons = document.querySelectorAll('button[wire\\:key^="paginator-page-page"]');
            const pageNumbers = Array.from(buttons).map(btn => parseInt(btn.textContent.trim(), 10)).filter(num => !isNaN(num));
            const maxPage = Math.max(...pageNumbers);
            const rows = document.querySelectorAll('tbody[data-flux-rows] tr[data-flux-row]');
            const packs = [];
            rows.forEach((row) => {
                const linkElement = row.querySelector('td[data-flux-cell] a[href*="/starter-pack/"]');
                if (linkElement) {
                    const url = linkElement.href.trim();
                    const urlParts = url.split('/');
                    const rkey = urlParts.pop();
                    const creatorHandle = urlParts.pop();
                    if (creatorHandle && rkey) {
                        packs.push(`${creatorHandle}|${rkey}`);
                    }
                }
            });
            return packs;
        });

        // Write found packs to the output file
        for (const pack of pagePacks) {
            urlStream.write(`${pack}\n`);
        }

        console.log(`Page ${pageNum} scraped successfully with ${pagePacks.length} packs found.`);
        await page.close();
        return pagePacks.length;
    } catch (err) {
        console.error(`Error on page ${pageNum}:`, err.message);
        await page.close();

        if (retries < MAX_RETRIES - 1) {
            console.log(`Retrying page ${pageNum} (${retries + 2}/${MAX_RETRIES})...`);
            await delay(2000 * (retries + 1)); // Exponential backoff
            return await processPage(browser, pageNum, retries + 1);
        } else {
            console.error(`Failed to scrape page ${pageNum} after ${MAX_RETRIES} attempts.`);
            failedPagesStream.write(`Page ${pageNum} failed after ${MAX_RETRIES} attempts.\n`);
            return 0;
        }
    }
}

/**
 * Collect starter pack URLs with enhanced robustness
 */
async function collectStarterPackUrls() {
    // Clear or create the output files at the start
    fs.writeFileSync(OUTPUT_FILE, '', 'utf-8');
    fs.writeFileSync(FAILED_PAGES_FILE, '', 'utf-8');

    const browser = await puppeteer.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    let TOTAL_PAGES = 0;

    try {
        // Determine total number of pages dynamically
        const initialPage = await browser.newPage();
        await initialPage.setRequestInterception(true);
        initialPage.on('request', (req) => {
            const resourceType = req.resourceType();
            if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
                req.abort();
            } else {
                req.continue();
            }
        });

        console.log('Determining total number of pages...');
        await initialPage.goto('https://blueskydirectory.com/starter-packs/all?page=1', {
            waitUntil: 'networkidle2',
            timeout: 60000,
        });

        // Wait for the paginator buttons to load
        await initialPage.waitForSelector('button[wire\\:key^="paginator-page-page"]', { timeout: 30000 });

        TOTAL_PAGES = await initialPage.evaluate(() => {
            const buttons = document.querySelectorAll('button[wire\\:key^="paginator-page-page"]');
            const pageNumbers = Array.from(buttons).map(btn => parseInt(btn.textContent.trim(), 10)).filter(num => !isNaN(num));
            const maxPage = Math.max(...pageNumbers);
            return isFinite(maxPage) ? maxPage : 1;
        });

        console.log(`Total Pages Found: ${TOTAL_PAGES}`);
        await initialPage.close();

        if (TOTAL_PAGES === 1) {
            console.warn('Only 1 page detected. Please verify the pagination selectors or site structure.');
        }

        // Process pages in batches with concurrency control
        for (let i = 1; i <= TOTAL_PAGES; i += CONCURRENT_PAGES) {
            const batchPromises = [];
            const currentBatchSize = Math.min(CONCURRENT_PAGES, TOTAL_PAGES - i + 1);

            for (let j = 0; j < currentBatchSize; j++) {
                const pageNum = i + j;
                batchPromises.push(processPage(browser, pageNum));
            }

            // Await all promises in the current batch
            const results = await Promise.all(batchPromises);

            // Optional: Implement a delay between batches to prevent overloading
            await delay(DELAY_BETWEEN_BATCHES);

            // Aggregate total packs found
            const packsFound = results.reduce((acc, count) => acc + count, 0);
            console.log(`Batch ${Math.floor(i / CONCURRENT_PAGES) + 1}: Found ${packsFound} packs.`);
        }

        console.log('Scraping completed.');
    } catch (err) {
        console.error('Unexpected error during scraping:', err.message);
    } finally {
        await browser.close();
        urlStream.end();
        failedPagesStream.end();
    }
}

// Execute the URL collector
collectStarterPackUrls().catch((err) => {
    console.error('Fatal error:', err);
});
