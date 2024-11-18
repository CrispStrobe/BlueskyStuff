- [Tips](#tips)
  * [Account Security](#account-security)
  * [Custom Domains](#custom-domains)
  * [Content Discovery](#content-discovery)
    + [Starter Packs](#starter-packs)
    + [Custom Feeds](#custom-feeds)
- [Tools](#tools)
  * [Analytics](#analytics)
  * [Custom Feeds](#custom-feeds-1)
    + [Useful Custom Feeds](#useful-custom-feeds)
  * [Starter Packs](#starter-packs-1)
  * [Useful Starter Packs](#useful-starter-packs)
  * [Content Importers](#content-importers)
  * [Labelers](#labelers)
- [Clients](#clients)
  * [Web](#web)
  * [IOS/IpadOS](#ios-ipados)
  * [Android](#android)
- [Integration with other social media services](#integration-with-other-social-media-services)
  * [Service Bridges](#service-bridges)
  * [Cross-Posting Tools](#cross-posting-tools)
    + [Publishing Tools](#publishing-tools)
    + [Aggregation Tools](#aggregation-tools)
- [Social Media Cleanup](#social-media-cleanup)
- [Other sites related to Bluesky](#other-sites-related-to-bluesky)
  * [Directories](#directories)
  * [Discord Servers](#discord-servers)
    
# Bluesky Guide
Collection of Tips &amp; Tools for collaborating on [Bluesky](https://bsky.app) 
- amended from the version by [Jeftek.com](https://bsky.app/profile/jeftek.com)

# Tips
## Account Security
- **Use a Unique Long Password** -  Be sure to use a unique and long password such as a pass phrase for your account.
  * Think "My Dog has 2 Black Spots On their tail" is better than "BlueWinter24!"
- **Enable MFA for login** -  Currently Bluesky only offers email (You do require MFA on your email right?) based secondary verification when signining in with your main password, which can be enabled in your account settings.  (I hope to see Passkey support once they enable OAUTH.)
  * Settings > Two-factor authentication >  Enable Require email code to log into your account
- **Use Unique App Passwords for 3rd party apps** - When signing into 3rd party apps and services, you can generate an App Password for a specific app.
  * Settings > Advanced > App Passwords
## Custom Domains
Bluesky allows you to set your owned custom domain name as your handle/username, and is a great way to demonstrate your account is associated with your public web presence.
- [How to ser your domain as your handle](https://bsky.social/about/blog/4-28-2023-domain-handle-tutorial) - A guide on using a custom domain as your Bluesky handle
- [Purchase Domain for Bluesky account](https://bsky.social/about/blog/7-05-2023-namecheap) - If you don't already own your own domain, you can purchase one straight through Bluesky and Namecheap
## Content Discovery
When you first join, and are not following people Bluesky can be a lonely space.   Luckily there are plenty of tools and services for discovering accounts and content to follow to get engaged.

## Safety
### Posts on Bluesky Safety
- [A great list tips on blocking and moderation from @francesmeh.reviews](https://bsky.app/profile/francesmeh.reviews/post/3lamkrhv6k22o)

### Starter Packs
Starter Packs are curated collections of accounts you can review to follow or follow them all at once. 

### Custom Feeds
You see content based on who you follow, but you also can subscribe to custom feeds that people create on a whole variety of topics.   You can then choose to pin these to your home feed and switch between them like channels.  This allows you to choose not to follow accounts, but subscribe to feeds which may include those accounts so you can switch your viewing almost like channels.
Like Starter Packs, people can choose to create custom feeds they publish on their Bluesky profile, so if you find someone interesting you follow check out their profile and see if they are publishing any.  

## Content Optimization
Your timeline is initially filled with who you follow, but you don't have to follow everyone to see their content.  You may choose to limit who follow, but instead add them to your personal lists that you can switch to to see posters in that lists topic.  You can easily pin lists to your home page and switch between them for different feeds or channels of related content. Combining this with public feeds, you can keep your main following optimized and have different channels to swap to from your main timeline view.  
- A good example is choosing not follow News sources, but instead add them to a custom list or follow a public feed that includes them so you only view the content when you switch to it.

# Tools
A collection of useful tools for extending the collaboration experience on Bluesky

You can find many of these also on the official Bluesky docs in their showcase of projects built on the Bluesky API at [Bluesky Showcase](docs.bsky.app/showcase)
Other collections (some outdated) include: [awesome bluesky](https://github.com/notjuliet/awesome-bluesky)

## Analytics
Tools to give you insights into activity and engagement on Bluesky

- [User Count](https://bsky-users.theo.io/)
- [Blue View](https://blueview.app) - Used to track analytics about a Bluesky profile such as followers, engagement, etc.
- [Clearsky](https://clearsky.app) - View the activity on Bluesky, and specific profiles to see what lists, blocks, and history of a profile
  * See who blocks a profile
  * See what lists/feeds include the profile
  * See changes in the account over time 
- [Bluesky Atmosphere Stats](https://jyc.dev/at) - Look at analytics for a specific profile such as likes, posts, reposts and insights about engagement behavior
- 
### Network Visualizers
Tools that create visuals of the relationships across the Bluesky network
- [Aurora](https://aurora.ndimensional.xyz/)
- 
## Custom Feeds
Tools for creating and curating custom feeds that you can share on your Bluesky profile
- [Bluesky Feed Creator](https://blueskyfeedcreator.com) - Allows you to create your custom feeds to capture posts and publish to your profile.
### Useful Custom Feeds
- [📌 to Bookmark](https://bsky.app/profile/did:plc:q6gjnaw2blty4crticxkmujt/feed/my-pins) - Bluesky currently doesn't have native Bookmarks, but you can reply to a post with a 📌 (pin) emoji, and it will show up in this custom feed for you go back to later

## Starter Packs
When you first join Bluesky it can feel empty, but that is because you may not have discovered content to follow. The Starter Packs feature allows people to curate and publish collections of accounts around a specific topic or theme.  You can choose to view the list to follow all of them or follow some of them, and start building your timeline.
## Useful Starter Packs
 - [BlueSkyDirectory](https://blueskydirectory.com/starter-packs/all) - comprehensive searchable index
 - [Skypacks Collections](https://bsky.app/profile/skypacks.bsky.social) - Skypacks publishes useful starter packs for people to discovery from all sorts of topics.

## Content Importers
Tools for importing content from other platforms to your Bluesky timeline

- [Porto](https://chromewebstore.google.com/detail/porto-port-your-tweets-to/ckilhjdflnaakopknngigiggfpnjaaop?authuser=2&hl=en) - A Chrome extension build by [Ankit Bhandari](https://bsky.app/profile/anku.bsky.social) to use your X Profile export to import into your Bluesky timeline.
- [Blueark App](https://blueark.app/) - Service to import your Tweet/X exports to be imported to Bluesky timeline
- [Sky Follower Bridge](https://chromewebstore.google.com/detail/sky-follower-bridge/behhbpbpmailcnfbjagknjngnfdojpko) - Chrome Extension to review your X followers and add them on Bluesky if they exist
  
## Labelers
A collection of custom labelers that enabling labeling posts and profiles which may allow you to hide or augment for visibility

- [Anti-Alf Aktion](https://bsky.app/profile/did:plc:e4elbtctnfqocyfcml6h2lf7) - A collection of custom labelers across different categories

# Clients
Alternative clients for engaging on the Bluesky platform

## Web
- [Skyfeed](https://skyfeed.app/) - especially useful for feed management
  * [Github repo](https://github.com/skyfeed-dev/app)
- [Blue Deck](https://deck.blue) - multi-column layout
- [TOKIMEKI](https://tokimeki.blue/login)
  * [Github repo](https://github.com/spuithori/tokimekibluesky)

## IOS/IpadOS
- [Skeets App](https://www.skeetsapp.com) - Mobile Bluesky client from [SkeetsApp.com](https://bsky.app/profile/skeetsapp.com)
- [Graysky](https://graysky.app/) - A mobile client for engaging on Bluesky for IOS and Android
  * [Github repo](https://github.com/mozzius/graysky)

## Android
- [Graysky](https://graysky.app/) - A mobile client for engaging on Bluesky for IOS and Android
  * [Github repo](https://github.com/mozzius/graysky)
 
## MacOS
- [Sky.app (Github repo)](https://github.com/jcsalterego/Sky.app)
- [SwiftSky (Github repo)](https://github.com/rmcan/swiftsky) (quite rudimentary atm)

## CommandLine
- [mattn/bsky (Github repo](https://github.com/mattn/bsky)

# Integration with other social media services

## Service Bridges
These tools allow you to bridge networks together so posts flow from one to the other allowing users to post in 1 eco system and have the content appear in others
- [Bridgy Fed](https://fed.brid.gy/) - Bridges the fediverse and Bluesky

## Cross-Posting Tools
These are a collection of tools that exist to allow people to post the same content across multiple services

### Publishing Tools
These allow you to post content and monitor analytics across social media networks
- [Buffer](https://buffer.com) - Allows you to post content to multiple social media services at once, monitor and schedule posts
- [Croissant](https://apps.apple.com/us/app/croissant-cross-posting/id6670288979) - IOS app that allows you to post to Bluesky, Mastodon, and Threads at the same time
- [fedica](https://fedica.com/) - Social Media publishing and analytics platform for cross-posting across networks

### Aggregation Tools
- [OpenVibe](https://apps.apple.com/us/app/openvibe-mastodon-bluesky/id1666230916) - IOS App to post and consume feeds across Bluesky, Mastodon, and Nostr social media networks in one mobile interface  

# Social Media Cleanup
Tools for removing, pruning your social media services posts and followers

- [Redact](https://redact.dev/) - A tool that supports many popular social networks to allow you to prune or mass delete content and account your follow

# Other sites related to Bluesky

## Directories
- [Bluesky Directory](https://blueskydirectory.com) - A collection of profiles, applications and tools related to Bluesky

## Discord Servers
- [Bluesky API Touchers](https://discord.gg/FS9U8A7F)
- [Bluesky Community Services](https://discord.gg/tYuDvuzbVA)
- [AT Protocol PDS Admins](discord.gg/eT6RWzy) - AT Protocol PDS Admins


---------
Disclaimer:  I am not associated with any of these tools, posts, or sites and use of them is at your own risk.  Be sure to follow good security practicies with your account credentials and app passwords (until we get OAUTH!).   **Please enable MFA on your Bluesky account which you can find in settings.**

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>
