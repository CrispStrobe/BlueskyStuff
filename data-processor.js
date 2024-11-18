require('dotenv').config();
const { BskyAgent } = require('@atproto/api');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const yaml = require('js-yaml');
const winston = require('winston');

// Configure winston for advanced logging
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level.toUpperCase()}]: ${message}`)
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'data_processor.log' }),
    ],
});

class StarterPackProcessor {
    constructor() {
        this.agent = new BskyAgent({ service: 'https://bsky.social' });
        this.mongoClient = new MongoClient(process.env.MONGODB_URI);
        this.db = null;

        // Initialize write streams for incremental writing
        this.jsonStream = fs.createWriteStream('starter_packs.json', { flags: 'w' });
        this.yamlStream = fs.createWriteStream('starter_packs.yaml', { flags: 'w' });

        // Initialize JSON array
        this.jsonStream.write('[\n');
        this.isFirstJsonEntry = true;
    }

    async setupDatabase() {
        try {
            await this.mongoClient.connect();
            this.db = this.mongoClient.db('starterpacks');
            await this.db.collection('users').createIndex({ did: 1, pack_id: 1 }, { unique: true });
            await this.db.collection('users').createIndex({ handle: 1 });
            await this.db.collection('users').createIndex({ pack_id: 1 });
            logger.info('Connected to MongoDB and indexes created.');
        } catch (err) {
            logger.error(`Error setting up the database: ${err.message}`);
            process.exit(1);
        }
    }

    async setupAgent() {
        try {
            await this.agent.login({
                identifier: process.env.BSKY_USERNAME,
                password: process.env.BSKY_PASSWORD,
            });
            logger.info('Authenticated with BskyAgent.');
        } catch (err) {
            logger.error(`Error authenticating BskyAgent: ${err.message}`);
            process.exit(1);
        }
    }

    /**
     * Sanitize handle before resolution
     * @param {string} handle - The handle to sanitize
     * @returns {string} - Sanitized handle
     */
    sanitizeHandle(handle) {
        // Remove any trailing .bsky.social if present
        handle = handle.replace(/\.bsky\.social$/, '');

        // Remove any protocol prefixes if present
        handle = handle.replace(/^(http:\/\/|https:\/\/)/, '');

        // Remove any trailing periods
        handle = handle.replace(/\.$/, '');

        // Ensure the handle format is valid
        if (!handle.includes('.')) {
            handle = `${handle}.bsky.social`;
        }

        return handle.toLowerCase();
    }

    async getListMembers(uri) {
        try {
            logger.info(`Fetching list members for: ${uri}`);

            // The correct API call format based on the Bluesky repo
            const response = await this.agent.api.app.bsky.graph.getList({
                list: uri,
                limit: 100
            });

            logger.info(`Raw API response: ${JSON.stringify(response, null, 2)}`);

            if (!response?.data?.items) {
                logger.error(`No items found in getList response for ${uri}`);
                return [];
            }

            const members = response.data.items;
            logger.info(`Found ${members.length} members in list`);
            if (members.length > 0) {
                logger.info(`Sample member structure: ${JSON.stringify(members[0], null, 2)}`);
            }
            
            return members;
        } catch (err) {
            logger.error(`Error getting list members for ${uri}: ${err.message}`);
            if (err.response) {
                logger.error(`Error response: ${JSON.stringify(err.response.data || err.response, null, 2)}`);
            }
            return [];
        }
    }

    async getProfile(did) {
        try {
            await this.rateLimiter.throttle();
            logger.info(`Fetching profile for DID: ${did}`);
            
            // Use the correct API format for profile fetching
            const response = await this.agent.api.app.bsky.actor.getProfile({ actor: did });
            
            if (response?.data) {
                logger.info(`Profile found for ${did}: ${response.data.handle}`);
                return response.data;
            } else {
                logger.error(`No profile data returned for ${did}`);
                return null;
            }
        } catch (err) {
            logger.error(`Error getting profile for ${did}: ${err.message}`);
            return null;
        }
    }

    async getRecord(uri) {
        const [repo, collection, rkey] = uri.replace('at://', '').split('/');
        try {
            logger.info(`Fetching record: ${uri}`);
            // Use the correct API format for record fetching
            const record = await this.agent.api.com.atproto.repo.getRecord({
                repo,
                collection,
                rkey
            });
            
            if (!record?.data?.value) {
                logger.error(`No value found in record response for ${uri}`);
                return null;
            }
            
            logger.info(`Record structure: ${JSON.stringify(record.data, null, 2)}`);
            
            return record.data;
        } catch (err) {
            logger.error(`Error getting ${uri}: ${err.message}`);
            return null;
        }
    }
    
    /**
     * Resolves a handle to its DID with retry mechanism
     * @param {string} rawHandle - The handle to resolve
     * @param {number} retries - Current retry count
     * @returns {Promise<string|null>} - The DID or null if failed
     */
    async resolveHandleWithRetry(rawHandle, retries = 0) {
        const MAX_RETRIES = 3;
        const RETRY_DELAY = 2000; // 2 seconds

        try {
            const handle = this.sanitizeHandle(rawHandle);
            logger.info(`Attempting to resolve sanitized handle: ${handle}`);

            const response = await this.agent.resolveHandle({ handle });

            if (response?.data?.did) {
                return response.data.did;
            } else {
                throw new Error('No DID found in the response.');
            }
        } catch (err) {
            logger.error(`Error resolving handle ${rawHandle}: ${err.message}`);
            if (retries < MAX_RETRIES - 1) {
                logger.info(`Retrying handle resolution for ${rawHandle} (${retries + 2}/${MAX_RETRIES})...`);
                await this.delay(RETRY_DELAY * (retries + 1)); // Exponential backoff
                return await this.resolveHandleWithRetry(rawHandle, retries + 1);
            } else {
                logger.error(`Failed to resolve handle ${rawHandle} after ${MAX_RETRIES} attempts.`);
                return null;
            }
        }
    }

    async getProfile(did) {
        try {
            const response = await this.agent.getProfile({ actor: did });
            return response.data;
        } catch (err) {
            logger.error(`Error getting profile for ${did}: ${err.message}`);
            return null;
        }
    }

    async processStarterPack(urlLine) {
        // Validate and clean the input line
        if (!urlLine || !urlLine.includes('|')) {
            logger.error(`Invalid URL line format: ${urlLine}`);
            return;
        }

        const [creatorHandle, rkey] = urlLine.trim().split('|').map(s => s.trim());
        if (!creatorHandle || !rkey) {
            logger.error(`Missing handle or rkey in line: ${urlLine}`);
            return;
        }

        logger.info(`Processing pack by ${creatorHandle}: ${rkey}`);

        // Resolve creatorHandle to DID with retry
        let creatorDID = await this.resolveHandleWithRetry(creatorHandle);
        if (!creatorDID) {
            logger.error(`Skipping pack ${rkey} due to unresolved DID.`);
            return;
        }

        const recordUri = `at://${creatorDID}/app.bsky.graph.starterpack/${rkey}`;
        const record = await this.getRecord(recordUri);
        if (!record) {
            logger.error(`Could not fetch starter pack record: ${recordUri}`);
            return;
        }

        const { value } = record;
        logger.info(`Pack name: ${value.name}`);
        logger.info(`Description: ${value.description}`);
        logger.info(`List URI: ${value.list}`);

        // Get list members using our method
        const listMembers = await this.getListMembers(value.list);
        
        if (!listMembers || listMembers.length === 0) {
            logger.error(`No members found in the list for pack ${rkey}`);
            return;
        }

        logger.info(`Processing ${listMembers.length} users...`);
        const usersCollection = this.db.collection('users');

        const profilePromises = listMembers.map(async (member) => {
            // Debug log the member structure
            logger.info(`Processing member: ${JSON.stringify(member, null, 2)}`);

            // Extract the DID from the member object
            const memberDid = member.did || (member.subject ? member.subject.did : null);
            
            if (!memberDid) {
                logger.error(`Could not extract DID from member: ${JSON.stringify(member)}`);
                return null;
            }

            const profile = await this.getProfile(memberDid);
            if (profile) {
                return {
                    updateOne: {
                        filter: { did: memberDid, pack_id: rkey },
                        update: {
                            $set: {
                                did: memberDid,
                                handle: profile.handle,
                                display_name: profile.displayName,
                                pack_id: rkey,
                                pack_name: value.name,
                                pack_creator: creatorHandle,
                                added_at: new Date()
                            }
                        },
                        upsert: true
                    }
                };
            }
            return null;
        });

        try {
            const operations = (await Promise.all(profilePromises)).filter(op => op !== null);
            
            if (operations.length > 0) {
                const result = await usersCollection.bulkWrite(operations);
                logger.info(`Saved ${operations.length} users for pack ${rkey}`);
                logger.info(`MongoDB result: ${JSON.stringify(result.result || result, null, 2)}`);

                const packData = await usersCollection.aggregate([
                    { $match: { pack_id: rkey } },
                    {
                        $group: {
                            _id: { pack_id: "$pack_id", pack_name: "$pack_name", pack_creator: "$pack_creator" },
                            user_count: { $sum: 1 },
                            users: { 
                                $push: { 
                                    handle: "$handle", 
                                    display_name: "$display_name",
                                    did: "$did"
                                }
                            }
                        }
                    }
                ]).toArray();

                if (packData.length > 0) {
                    const pack = packData[0];
                    const outputEntry = {
                        name: pack._id.pack_name,
                        creator: pack._id.pack_creator,
                        rkey: pack._id.pack_id,
                        url: `https://bsky.app/profile/${creatorHandle}/lists/${rkey}`,
                        user_count: pack.user_count,
                        users: pack.users.map(user => ({
                            handle: user.handle,
                            display_name: user.display_name || '',
                            did: user.did
                        }))
                    };

                    // Write to JSON stream
                    if (!this.isFirstJsonEntry) {
                        this.jsonStream.write(',\n');
                    } else {
                        this.isFirstJsonEntry = false;
                    }
                    this.jsonStream.write(JSON.stringify(outputEntry, null, 2));

                    // Write to YAML stream
                    this.yamlStream.write('---\n');
                    this.yamlStream.write(yaml.dump(outputEntry));
                }
            } else {
                logger.error(`No valid operations generated for pack ${rkey}`);
            }
        } catch (err) {
            logger.error(`Error saving users for pack ${rkey}: ${err.message}`);
        }
    }

    async exportData(format = 'json') {
        // Since we're writing incrementally, finalize the JSON array
        if (format === 'json') {
            this.jsonStream.write('\n]\n');
            this.jsonStream.end();
        }

        // For YAML, just end the stream
        if (format === 'yaml') {
            this.yamlStream.end();
        }

        logger.info(`Exported data to starter_packs.${format}`);
    }

    async processUrls(filename) {
        if (!fs.existsSync(filename)) {
            logger.error(`Input file ${filename} not found`);
            return;
        }

        const urls = fs.readFileSync(filename, 'utf-8')
            .split('\n')
            .map(line => line.trim())
            .filter(line => line && line.includes('|'));

        logger.info(`Processing ${urls.length} starter packs...`);

        let startIndex = 0;
        if (fs.existsSync('checkpoint.txt')) {
            const lastIndex = parseInt(fs.readFileSync('checkpoint.txt', 'utf-8'), 10);
            startIndex = isNaN(lastIndex) ? 0 : lastIndex + 1;
            logger.info(`Resuming from index ${startIndex}`);
        }

        for (const [index, urlLine] of urls.entries()) {
            if (index < startIndex) continue; // Skip already processed
            await this.processStarterPack(urlLine.trim());

            // Update checkpoint after each pack
            fs.writeFileSync('checkpoint.txt', index.toString(), 'utf-8');

            // Rate limiting protection
            if (index % 10 === 0 && index !== 0) {
                logger.info('Adding small delay to prevent rate limiting...');
                await this.delay(1000);
            }

            // Longer pause every 100 items
            if (index % 100 === 0 && index !== 0) {
                logger.info('Adding longer delay to prevent rate limiting...');
                await this.delay(5000);
            }
        }

        // Finalize the output files
        await this.exportData('json');
        await this.exportData('yaml');

        // Remove checkpoint file after completion
        if (fs.existsSync('checkpoint.txt')) {
            fs.unlinkSync('checkpoint.txt');
        }
    }

    async collect() {
        await this.setupDatabase();
        await this.setupAgent();
        await this.processUrls('starter_pack_urls.txt');
        await this.mongoClient.close();
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

async function main() {
    const processor = new StarterPackProcessor();
    await processor.collect();
}

main().catch(err => {
    logger.error('Fatal error:', err);
    process.exit(1);
});