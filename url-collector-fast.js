const puppeteer = require('puppeteer');
const fs = require('fs');

async function collectStarterPackUrls() {
    const browser = await puppeteer.launch();
    const urls = new Set();
    const CONCURRENT_PAGES = 5;
    const TOTAL_PAGES = 3030;
    
    for (let i = 0; i < TOTAL_PAGES; i += CONCURRENT_PAGES) {
        const pagePromises = [];
        for (let j = 0; j < CONCURRENT_PAGES && (i + j) <= TOTAL_PAGES; j++) {
            const pageNum = i + j + 1;
            pagePromises.push((async () => {
                const page = await browser.newPage();
                await page.setRequestInterception(true);
                page.on('request', (req) => {
                    if (['image', 'stylesheet', 'font'].includes(req.resourceType())) 
                        req.abort();
                    else 
                        req.continue();
                });

                console.log(`Scraping page ${pageNum}/${TOTAL_PAGES}`);
                try {
                    await page.goto(`https://blueskydirectory.com/starter-packs/all?page=${pageNum}`, {
                        waitUntil: 'domcontentloaded'
                    });

                    const pageUrls = await page.evaluate(() => {
                        return Array.from(
                            document.querySelectorAll('td a[href*="/starter-pack/"]'),
                            a => `https://bsky.app/starter-pack/${a.href.split('/').pop()}`
                        );
                    });

                    pageUrls.forEach(url => urls.add(url));
                    await page.close();
                    return pageUrls.length;
                } catch (err) {
                    console.error(`Error on page ${pageNum}:`, err);
                    return 0;
                }
            })());
        }

        const results = await Promise.all(pagePromises);
        console.log(`Batch complete, found ${results.reduce((a,b) => a+b, 0)} URLs (total: ${urls.size})`);
    }

    await browser.close();
    const urlList = Array.from(urls).join('\n');
    fs.writeFileSync('starter_pack_urls.txt', urlList);
    console.log(`\nSaved ${urls.size} unique URLs to starter_pack_urls.txt`);
}

collectStarterPackUrls().catch(console.error);
