
/*

//ADS platforms  => Data Ingestion stream  =>  Stream processing  =>  Data storage(ClickHouse/S3)  =>  API Layer => FE Layer


// 1. Data Ingestion stream:
// 2. Stream processing:
// 3. Data storage: Relational DB, S3 bucket
// 4. API Layer: 
// 5. Data visualization:

/* For building fault tolerant system we can integrate kafka where payload with Campaign id and Client id is ingested in topic.
A consumer can asyncronously pull data and fetch corresponding engagement data from Clickhouse db in chunks and uploads the processed data into S3 bucket.
I have included helper functions for the same with retry mechanism.

*/

//Sample DB queries for database creation

CREATE TABLE campaigns (
    campaign_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    start_date DATE NOT NULL,
    end_date DATE,
    budget DECIMAL(12, 2),
    status VARCHAR(50) CHECK (status IN ('planned', 'active', 'paused', 'completed'))
);

CREATE TABLE channels (
    channel_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE campaign_channels (
    campaign_channel_id SERIAL PRIMARY KEY,
    campaign_id INT REFERENCES campaigns(campaign_id),
    channel_id INT REFERENCES channels(channel_id)
);

CREATE TABLE events (
    event_id SERIAL PRIMARY KEY,
    campaign_id INT REFERENCES campaigns(campaign_id),
    channel_id INT REFERENCES channels(channel_id),
    audience_id INT REFERENCES audiences(audience_id),
    event_type VARCHAR(50) CHECK (event_type IN ('impression', 'click', 'conversion')),
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(255)
);

Engagement data Calculation

//Total Clicks per Campaign

SELECT c.name AS campaign_name, COUNT(e.event_id) AS total_clicks
FROM campaigns c
JOIN events e ON c.campaign_id = e.campaign_id
WHERE e.event_type = 'click'
GROUP BY c.name;



//Conversion Rate per Campaign

SELECT 
    c.name AS campaign_name,
    COUNT(CASE WHEN e.event_type = 'conversion' THEN 1 END)::FLOAT / NULLIF(COUNT(CASE WHEN e.event_type = 'click' THEN 1 END), 0) AS conversion_rate
FROM campaigns c
JOIN events e ON c.campaign_id = e.campaign_id
GROUP BY c.name;



//Top Performing Channels by Conversions

SELECT ch.name AS channel_name, COUNT(e.event_id) AS total_conversions
FROM channels ch
JOIN events e ON ch.channel_id = e.channel_id
WHERE e.event_type = 'conversion'
GROUP BY ch.name
ORDER BY total_conversions DESC;





















