// from ../: Compress-Archive -Path validateToken\* -DestinationPath validateTokenEndpoint.zip -Force
import pkg from 'pg';
const { Pool } = pkg;
import crypto from 'crypto'
import dotenv from 'dotenv'

dotenv.config();

const TOKEN_SECRET = process.env.TOKEN_SECRET;
const ALGORITHM = 'aes-256-cbc';

const headers = {'Content-Type':'application/json',
                    'Access-Control-Allow-Origin':'*',
                    'Access-Control-Allow-Methods':'POST,PATCH,OPTIONS'}

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,  
    port: 5432,                         
});

function decryptToken(encryptedToken) {
    const parts = encryptedToken.split(':');
    const iv = Buffer.from(parts[0], 'hex');
    const encryptedText = parts[1];
    
    const decipher = crypto.createDecipheriv(ALGORITHM, TOKEN_SECRET, iv);
    let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return JSON.parse(decrypted);
}

export const handler = async (event) => {
    try {
        console.log("event: ", event);

        if (!event.alpbToken || !event.widgetId) {
            return {
                statusCode: 400,
                headers,
                body: {
                    success: false,
                    message: "Missing required field(s) in request body"
                }
            }
        }
        const { alpbToken, widgetId: publicWidgetId } = event;
        const { userId, publicWidgetId: reqPublicWidgetId, sessionId } = decryptToken(alpbToken);

        console.log({alpbToken, publicWidgetId, reqPublicWidgetId, sessionId})

        // Check that a valid session exists w/ the user's id
        // For extra security: check the widgetId too?
        const sessionRes = await pool.query(
            "SELECT sess FROM session WHERE sid = $1",
            [sessionId]
        );

        console.log({sessionRes})

        if (sessionRes.rowCount === 0 || sessionRes.rows[0]["sess"]["user"]["user_id"] !== userId || publicWidgetId !== reqPublicWidgetId) {
            return {
                statusCode: 403,
                headers,
                body: {
                    success: false,
                    message: "Unauthorized"
                }
            }
        }

        return {
            statusCode: 200,
            headers,
            body: {
                success: true,
                message: "Token validated successfully"
            }
        }

    } catch (error) {
        return {
            statusCode: 500,
            headers,
            body: {
                success: false,
                message: "Could not validate token: " + error.message
            }
        }
    }
};
