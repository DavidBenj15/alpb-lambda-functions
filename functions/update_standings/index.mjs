// To deploy to lambda:
// cd <update_standings>
// zip -r update_standings.zip .
// aws lambda update-function-code --function-name update_standings --zip-file fileb://update_standings.zip

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import dotenv from 'dotenv';

dotenv.config();

const BASE_URL = process.env.POINTSTREAK_BASE;
const SEASON_ID = process.env.SEASON_ID;
const API_KEY = process.env.POINTSTREAK_API_KEY;
const SEASON_YEAR = process.env.SEASON_YEAR;
const BUCKET_NAME = process.env.STANDINGS_BUCKET_NAME;

const s3 = new S3Client({ region: "us-east-2" });

export const handler = async () => {
    try {
        // Fetch standings
        const response = await fetch(`${BASE_URL}/baseball/season/standings/${SEASON_ID}/json`, {
            headers: {
                "apikey": API_KEY
            }
        });
        const res = await response.json();
        res.year = SEASON_YEAR;
        res.updatedAt = new Date().toISOString();

        // Write to bucket
        const key = `standings/${SEASON_YEAR}-standings.json`;
        const command = new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: key,
            Body: JSON.stringify(res),
            ContentType: "application/json"
        });

        await s3.send(command);
        console.log(`✅ Successfully uploaded league standings to ${BUCKET_NAME}/${key}`);
        console.log(res);

        return {
            statusCode: 200,
            body: {
                success: true,
                message: `Successfully uploaded league standings to ${ bucketName }/${key}`
            }
        }
    } catch (error) {
        console.error(error);
        return {
            statusCode: 500,
            headers,
            body: {
                success: false,
                message: "Could not update standings: " + error.message
            }
        }
    }
    
};
