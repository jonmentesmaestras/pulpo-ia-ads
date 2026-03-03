const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const axios = require('axios');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
require('dotenv').config()

const app = express();
const port = 3001;
const router = express.Router();

app.use(cors());
app.use(express.json());
app.use('/', router);

// Ensure /ads/public/images directory exists
const imagesDir = '/home/zmcrbvch/public_html/images';
const imagesDir = "./public/images/";
if (!fs.existsSync(imagesDir)) {
    fs.mkdirSync(imagesDir, {recursive: true});
}

// Multer storage config
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, imagesDir);
    },
    filename: function (req, file, cb) {
        // Generate random 10-character name
        const randomName = Math.random().toString(36).substring(2, 12);
        const ext = path.extname(file.originalname).toLowerCase();
        cb(null, randomName + ext);
    }
});

const fileFilter = (req, file, cb) => {
    const allowedTypes = ['.png', '.jpg', '.jpeg'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowedTypes.includes(ext)) {
        cb(null, true);
    } else {
        cb(new Error('Only PNG and JPG images are allowed'));
    }
};

const upload = multer({storage, fileFilter});

// POST /ads/upload-image endpoint
router.post('/upload-image', upload.single('image'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({error: 'No image uploaded or invalid file type.'});
    }
    const filename = req.file.filename;
    const imageUrl = `https://tueducaciondigital.site/images/${filename}`;
    res.json({image: imageUrl});
});

const db = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const jsonFields = ['publisherPlatform', 'AdDescription', 'AdTitle', 'age', 'languages', 'countries', 'codeBelongs'];

function dateToEpochUnix(customeDate) {
    const date = new Date(customeDate);
    const unixTimeMillis = date.getTime();
    const unixTime = Math.floor(unixTimeMillis / 1000);
    return unixTime;
}

// Helper function to parse JSON fields and convert boolean
const parseJsonFields = (ad) => {
    const newAd = {...ad};

    if (newAd.hasOwnProperty('Active')) {
        newAd.Active = Boolean(newAd.Active);
    }

    for (const field of jsonFields) {
        if (newAd[field] && typeof newAd[field] === 'string') {
            try {
                newAd[field] = JSON.parse(newAd[field]);
            } catch (e) {
                console.error(`Error parsing JSON for field ${field}:`, e);
            }
        }
    }
    return newAd;
};

// Helpers for DB fields that are sometimes JSON and sometimes plain text
function asUtf8String(value) {
    if (value == null) return '';
    if (Buffer.isBuffer(value)) return value.toString('utf8');
    return String(value);
}

function parseJsonIfLooksLikeJson(value) {
    const str = asUtf8String(value).trim();
    if (!str) return {ok: false, raw: str, value: null};

    // Only attempt JSON.parse when it visually looks like JSON
    if (!(str.startsWith('[') || str.startsWith('{') || str.startsWith('"'))) {
        return {ok: false, raw: str, value: null};
    }

    try {
        return {ok: true, raw: str, value: JSON.parse(str)};
    } catch {
        return {ok: false, raw: str, value: null};
    }
}

function parseTextOrJsonArray(value) {
    const {ok, raw, value: parsed} = parseJsonIfLooksLikeJson(value);

    if (ok) {
        if (Array.isArray(parsed)) return parsed;
        if (typeof parsed === 'string' && parsed.trim() && parsed.trim().toUpperCase() !== 'N/A') return [parsed];
        return [];
    }

    // Not JSON: treat as plain text (unless N/A/empty)
    if (raw && raw.toUpperCase() !== 'N/A') return [raw];
    return [];
}

function parseJsonArrayOrEmpty(value) {
    const {ok, value: parsed} = parseJsonIfLooksLikeJson(value);
    return ok && Array.isArray(parsed) ? parsed : [];
}

function parseCountriesList(value) {
    const {ok, raw, value: parsed} = parseJsonIfLooksLikeJson(value);

    let str = raw;
    if (ok) {
        if (Array.isArray(parsed)) {
            return parsed
                .map(v => (v == null ? '' : String(v)).trim())
                .filter(Boolean);
        }
        if (typeof parsed === 'string') {
            str = parsed;
        } else {
            return [];
        }
    }

    if (!str) return [];

    // Support CSV/newline/semicolon separated lists.
    return str
        .split(/[\n,;]+/)
        .map(s => s.trim())
        .filter(Boolean);
}

function buildAnalisisAdsResponse(row) {
    return {
        'Idioma': row.Idioma || '',
        'Nicho de Mercado': row.Nicho || '',
        'País Potencial': parseCountriesList(row.Paises),
        'Público Objetivo': row.Publico || '',
        'Transcripción': row.Contenido_Video || '',
        'Ángulo del Copy': row.Angulo_Copy || '',
        'Analisis de antiguedad': row.Antiguedad || '',
        'Contenido persuasivo': row.Contenido_Persuasivo || '',
        'Problema/Deseo que Aborda': row.Problema || ''
    };
}

function toNullableTrimmedString(value) {
    if (value === undefined || value === null) return null;
    const str = String(value).trim();
    return str ? str : null;
}

function normalizePaisesForDb(value) {
    if (value === undefined || value === null) return null;
    if (Array.isArray(value)) {
        const cleaned = value
            .map(v => (v == null ? '' : String(v)).trim())
            .filter(Boolean);
        return cleaned.length ? JSON.stringify(cleaned) : null;
    }
    const str = String(value).trim();
    return str ? str : null;
}

function getBearerToken(req) {
    const auth = req.headers && req.headers.authorization ? String(req.headers.authorization) : '';
    const match = auth.match(/^Bearer\s+(.+)$/i);
    return match ? match[1].trim() : '';
}

// --- Active enrichment helpers (AWS multi-endpoint, 10 ids per request) ---
const AMAZON_ACTIVE_DETAILS_ENDPOINTS = [
    "https://lylfy0m6gg.execute-api.us-east-1.amazonaws.com/testVirginiaUno/getAdDetailsVirginia",
    "https://edb22vw54j.execute-api.sa-east-1.amazonaws.com/testeoSaoPablo/getdetailsAdsSaoPablo",
    "https://ja3go447e7.execute-api.us-east-2.amazonaws.com/getdatailstestOhio/getAdDetailsOhio",
    "https://yjuvd1utb0.execute-api.us-west-1.amazonaws.com/testeoCalif/getAdsDetailsCalif",
    "https://xneby1kkp3.execute-api.us-west-2.amazonaws.com/testGetDetailsOregon/getadsdetails"
];

function chunkArray(arr, size) {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
}

async function fetchActiveDetails(ids) {
    const unique = [...new Set(ids.filter(Boolean).map(String))];
    const chunks = chunkArray(unique, 10);
    const activeMap = new Map();
    // Pre-populate map with all requested IDs as inactive.
    for (const id of unique) {
        activeMap.set(id, false);
    }
    let endpointIndex = 0;

    for (const chunk of chunks) {
        const endpoint = "https://lylfy0m6gg.execute-api.us-east-1.amazonaws.com/testVirginiaUno/getAdDetailsVirginia"  //AMAZON_ACTIVE_DETAILS_ENDPOINTS[endpointIndex % AMAZON_ACTIVE_DETAILS_ENDPOINTS.length];
        endpointIndex++;
        try {
            const resp = await axios.get(endpoint, {
                params: {ids: chunk.join(',')},
                timeout: 20000
            });
            if (!resp) continue;

            let payload = resp.data;
            // Some gateways may return { statusCode, body }
            if (payload && typeof payload === 'object' && payload.body) {
                try {
                    payload = JSON.parse(payload.body);
                } catch (e) {
                    payload = [];
                }
            }

            // API only returns active ads. If an ad is in the payload, it's active.
            if (Array.isArray(payload)) {
                for (const item of payload) {
                    if (item && typeof item.LibraryID !== 'undefined' && item.Active === true) {
                        activeMap.set(String(item.LibraryID), true);
                    }
                }
            } else {
                console.warn('[ActiveEnrichment] Unexpected payload format from', endpoint);
            }
        } catch (e) {

            console.error('[ActiveEnrichment] Request failed for', endpoint, e && e.message ? e.message : e);
            console.log("ids chunks:", chunks);
        }
    }
    return activeMap;
}

// Endpoint to get records with filtering and pagination (refactored for ACTIVE flow)
router.get('/getads', async (req, res) => {
    const {
        keywords,
        landing,
        videoHost,
        pageBuilder,
        ad_reached_countries,
        ad_active_status,
        duplicates,
        cta,
        media_type,
        publisher_platforms,
        updated_date_min,
        updated_date_max,
        limit = 10,
        page = 1
    } = req.query;

    // Log incoming query for debugging CTA issue
    console.log('[GET /ads/getads] query params:', req.query);

    const isActiveRequested = ad_active_status && ad_active_status.toUpperCase() === 'ACTIVE';
    const hasDateRange = Boolean(updated_date_min && updated_date_max);

    let whereClauses = [];
    let params = [];
    let fulltextUsed = false;
    let booleanQuery = '';

    // Common filters
    if (keywords) {
        // Split, trim, dedupe tokens
        const rawTokens = [...new Set(keywords.split(/\s+/).map(t => t.trim()).filter(Boolean))];
        const fulltextTokens = [];
        const shortTokens = [];
        rawTokens.forEach(tok => {
            const sanitized = tok.replace(/[^0-9A-Za-zÀ-ÖØ-öø-ÿÁÉÍÓÚáéíóúÜüÑñÇç_-]/g, '');
            if (!sanitized) return;
            if (sanitized.length >= 3) fulltextTokens.push(sanitized); else shortTokens.push(sanitized);
        });
        // Use FULLTEXT when at least one token meets common ft_min_word_len
        if (fulltextTokens.length > 0) {
            fulltextUsed = true;
            const booleanParts = fulltextTokens.map(t => `${t}*`);
            booleanQuery = booleanParts.join(' ');
            // Parameterized MATCH (adds one placeholder "?")
            whereClauses.push('MATCH(__html, AdDescription_plain, AdTitle_plain, keywords) AGAINST (? IN BOOLEAN MODE)');
            params.push(booleanQuery); // keep order consistent with added where clause
        }
        // If FULLTEXT used, only short tokens get LIKE fallback; else all tokens use LIKE groups (AND semantics)
        const likeTokens = fulltextUsed ? shortTokens : rawTokens;
        likeTokens.forEach(tok => {
            const esc = tok.replace(/[%_]/g, m => '\\' + m);
            const like = `%${esc}%`;
            // Default backslash escaping is sufficient
            whereClauses.push('( __html LIKE ? OR AdDescription LIKE ? OR AdTitle LIKE ? OR keywords LIKE ? )');
            params.push(like, like, like, like);
        });
    }
    // Hybrid codeBelongs filtering:
    // (landing1 OR landing2 OR ...) AND (videoHost if provided) AND (pageBuilder if provided)
    (function () {
        // Landing: comma-separated -> OR group
        if (landing && typeof landing === 'string' && landing.trim() !== '' && landing.toUpperCase() !== 'ALL') {
            const landingTerms = [...new Set(landing.split(',').map(s => s.trim()).filter(s => s && s.toUpperCase() !== 'ALL'))];
            if (landingTerms.length === 1) {
                whereClauses.push('codeBelongs LIKE ?');
                params.push(`%"${landingTerms[0]}"%`);
            } else if (landingTerms.length > 1) {
                const orGroup = '(' + landingTerms.map(() => 'codeBelongs LIKE ?').join(' OR ') + ')';
                whereClauses.push(orGroup);
                landingTerms.forEach(t => params.push(`%"${t}"%`));
            }
        }
        // videoHost: mandatory presence (AND)
        if (videoHost && typeof videoHost === 'string' && videoHost.toUpperCase() !== 'ALL') {
            whereClauses.push('codeBelongs LIKE ?');
            params.push(`%"${videoHost}"%`);
        }
        // pageBuilder: mandatory presence (AND)
        if (pageBuilder && typeof pageBuilder === 'string' && pageBuilder.toUpperCase() !== 'ALL') {
            whereClauses.push('codeBelongs LIKE ?');
            params.push(`%"${pageBuilder}"%`);
        }
    })();
    // Refactored ad_reached_countries filter: only US or BR; else treat as ALL (fallback to 'ALL')
    if (ad_reached_countries) {
        const countryParamRaw = String(ad_reached_countries).trim().toUpperCase();
        let countryToUse = null;
        if (countryParamRaw === 'US' || countryParamRaw === 'BR') {
            countryToUse = countryParamRaw; // valid filter
        } else if (countryParamRaw !== 'ALL') {
            // Any other provided value forces ALL fallback (records containing "ALL")
            countryToUse = 'ALL';
        }
        if (countryToUse && countryToUse !== 'ALL') {
            whereClauses.push('countries LIKE ?');
            params.push(`%"${countryToUse}"%`);
        } else if (countryToUse === 'ALL') {
            // Explicitly search for entries marked as ALL (if such marker exists in stored array)
            whereClauses.push('countries LIKE ?');
            params.push('%"ALL"%');
        }
    }
    if (ad_active_status && ad_active_status.toUpperCase() !== 'ALL' && ad_active_status.toUpperCase() !== 'ACTIVE') {
        // Normal explicit Active filter (not the special ACTIVE enrichment case)
        whereClauses.push('Active = ?');
        params.push(ad_active_status === 'true' ? 1 : 0);
    }
    if (duplicates) {
        whereClauses.push('duplicates = ?');
        params.push(duplicates);
    }
    // New CTA filter
    if (cta && typeof cta === 'string' && cta.toUpperCase() !== 'ALL') {
        // Support multiple comma-separated CTA values
        const ctaValues = cta.split(',').map(s => s.trim()).filter(Boolean);
        if (ctaValues.length === 1) {
            whereClauses.push('cta_type = ?');
            params.push(ctaValues[0]);
        } else if (ctaValues.length > 1) {
            whereClauses.push(`cta_type IN (${ctaValues.map(() => '?').join(',')})`);
            params.push(...ctaValues);
        }
    }

    // New media_type filter
    if (media_type && typeof media_type === 'string' && media_type.toUpperCase() !== 'ALL') {
        let dbMedia = null;
        if (media_type.toUpperCase() === 'VIDEO') dbMedia = 'video';
        else if (media_type.toUpperCase() === 'IMAGE') dbMedia = 'img';
        if (dbMedia) {
            whereClauses.push('AdMedia = ?');
            params.push(dbMedia);
        }
    }

    // New publisher_platforms filter (accepts single or comma-separated list, case-insensitive)
    if (publisher_platforms && typeof publisher_platforms === 'string') {
        const plats = publisher_platforms.split(',').map(p => p.trim().toLowerCase()).filter(Boolean);
        if (plats.length === 1) {
            whereClauses.push('publisherPlatform LIKE ?');
            params.push(`%"${plats[0]}"%`);
        } else if (plats.length > 1) {
            const orConds = plats.map(() => 'publisherPlatform LIKE ?').join(' OR ');
            whereClauses.push(`(${orConds})`);
            plats.forEach(p => params.push(`%"${p}"%`));
        }
    }

    // Date filters (inclusive range if provided)
    if (updated_date_min) {
        const fechahoraInicio = `${updated_date_min}T00:00:00.000Z`;
        const di = dateToEpochUnix(fechahoraInicio);
        whereClauses.push('startDate >= ?');
        params.push(di);
    }
    if (updated_date_max) {
        const fechahoraFin = `${updated_date_max}T23:59:59.999Z`;
        const df = dateToEpochUnix(fechahoraFin);
        whereClauses.push('startDate <= ?');
        params.push(df);
    }

    const whereString = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

    // Dynamic select & ordering
    const selectColumns = fulltextUsed
        ? '*, MATCH(__html, AdDescription_plain, AdTitle_plain, keywords) AGAINST (? IN BOOLEAN MODE) AS score'
        : '*';
    // Always include adsdomains.id DESC for deterministic ordering/debugging.
    const orderByClause = hasDateRange
        ? (fulltextUsed
            ? 'ORDER BY startDate ASC, score DESC, `adsdomains`.`id` DESC'
            : 'ORDER BY startDate ASC, `adsdomains`.`id` DESC')
        : (fulltextUsed
            ? 'ORDER BY score DESC, `adsdomains`.`id` DESC'
            : 'ORDER BY `adsdomains`.`id` DESC');

    const countQuery = `SELECT COUNT(*) as totalItems
                        FROM adsdomains ${whereString}`;
    console.log('[GET /ads/getads] WHERE:', whereString, 'PARAMS:', params);
    db.query(countQuery, params, (err, countResult) => {
        if (err) {
            console.error('Error fetching records count:', err);
            return res.status(500).json({message: 'Error fetching records from database.', error: err});
        }
        const totalItems = countResult[0].totalItems;
        const totalPages = Math.ceil(totalItems / limit);
        const offset = (page - 1) * limit;

        const dataQuery = `SELECT ${selectColumns}
                           FROM adsdomains ${whereString} ${orderByClause}
                           LIMIT ? OFFSET ?`;
        // If fulltextUsed, we added one booleanQuery param inside WHERE (already in params) and need another for SELECT relevance
        const dataParams = fulltextUsed
            ? [booleanQuery, ...params, parseInt(limit), parseInt(offset)]
            : [...params, parseInt(limit), parseInt(offset)];

        console.log(' OJOOO [GET /ads/getads] DATA QUERY:', dataQuery, 'DATA PARAMS:', dataParams);

        db.query(dataQuery, dataParams, async (err, results) => {

            if (err) {
                console.error('Error fetching records:', err);
                return res.status(500).json({message: 'Error fetching records from database.', error: err});
            }

            // If ACTIVE requested, enrich Active via AWS details endpoints (batch <=10 ids, round-robin endpoints)
            if (isActiveRequested) {
                try {
                    const ids = results.map(r => r.LibraryID).filter(Boolean);
                    if (ids.length > 0) {
                        const activeMap = await fetchActiveDetails(ids);
                        // Update Active status on all results based on the map.
                        // If an ID is not in the map, it's considered inactive.
                        for (const r of results) {
                            r.Active = activeMap.get(String(r.LibraryID)) || false;
                        }
                    } else {
                        // If no IDs to check, mark all as inactive
                        for (const r of results) {
                            r.Active = false;
                        }
                    }
                } catch (e) {
                    console.error('[ActiveEnrichment] Error during enrichment:', e);
                    // On error, treat all as inactive to be safe
                    for (const r of results) {
                        r.Active = false;
                    }
                }
            }

            // If ACTIVE requested, return only active ads after enrichment
            const finalResults = isActiveRequested
                ? results.filter(r => r.Active === true)
                : results;
            return sendResponse(finalResults);

            function sendResponse(records) {
                const parsedResults = records.map(parseJsonFields);
                res.status(200).json({
                    message: "Success",
                    code: 200,
                    error: false,
                    status: "Ok",
                    stage: "Ending",
                    info: {
                        currentPage: parseInt(page),
                        totalPages: totalPages,
                        totalItems: totalItems,
                        hasNextPage: parseInt(page) < totalPages,
                        hasPrevPage: parseInt(page) > 1
                    },
                    after: parseInt(page) + 1,
                    data: parsedResults
                });
            }
        });
    });
});

// GET /ads/analisis-ads?libraryId=
// Returns analysis data from Analisis_Ads by FK_LibraryID
router.get('/analisis-ads', (req, res) => {
    const libraryId = (req.query && req.query.libraryId) ? String(req.query.libraryId).trim() : '';
    if (!libraryId) {
        return res.status(400).json({message: 'Missing libraryId query parameter.'});
    }

    const query = 'SELECT * FROM Analisis_Ads WHERE FK_LibraryID = ? ORDER BY ID DESC LIMIT 1';
    db.query(query, [libraryId], (err, results) => {
        if (err) {
            console.error('Error fetching Analisis_Ads:', err);
            return res.status(500).json({message: 'Error fetching analysis from database.', error: err});
        }
        if (!results || results.length === 0) {
            return res.status(404).json({message: 'Not found.'});
        }

        return res.status(200).json(buildAnalisisAdsResponse(results[0]));
    });
});

// POST /ads/analisis-ads
// Body: JSON with fields matching Analisis_Ads columns
// Behavior:
// - If FK_LibraryID already exists, returns the existing row
// - If not, inserts a new row and returns it
router.post('/analisis-ads', (req, res) => {
    const body = req.body || {};
    const libraryId = body.FK_LibraryID ? String(body.FK_LibraryID).trim() : '';

    if (!libraryId) {
        return res.status(400).json({message: 'FK_LibraryID is required.'});
    }

    const insertPayload = {
        FK_LibraryID: libraryId,
        Antiguedad: toNullableTrimmedString(body.Antiguedad),
        Contenido_Persuasivo: toNullableTrimmedString(body.Contenido_Persuasivo),
        Problema: toNullableTrimmedString(body.Problema),
        Publico: toNullableTrimmedString(body.Publico),
        Nicho: toNullableTrimmedString(body.Nicho),
        Paises: normalizePaisesForDb(body.Paises),
        Idioma: toNullableTrimmedString(body.Idioma),
        Angulo_Copy: toNullableTrimmedString(body.Angulo_Copy),
        Gancho_Video: toNullableTrimmedString(body.Gancho_Video),
        Contenido_Video: toNullableTrimmedString(body.Contenido_Video)
    };

    db.getConnection((connErr, conn) => {
        if (connErr) {
            console.error('Error getting DB connection:', connErr);
            return res.status(500).json({message: 'Database connection error.', error: connErr});
        }

        const rollbackAndRespond = (status, payload) => {
            conn.rollback(() => {
                conn.release();
                res.status(status).json(payload);
            });
        };

        conn.beginTransaction((txErr) => {
            if (txErr) {
                console.error('Error starting transaction:', txErr);
                conn.release();
                return res.status(500).json({message: 'Database transaction error.', error: txErr});
            }

            // Lock any existing row (or the gap) for this FK_LibraryID to prevent duplicates under concurrency.
            const selectSql = 'SELECT * FROM Analisis_Ads WHERE FK_LibraryID = ? ORDER BY ID DESC LIMIT 1 FOR UPDATE';
            conn.query(selectSql, [libraryId], (selectErr, rows) => {
                if (selectErr) {
                    console.error('Error checking existing Analisis_Ads:', selectErr);
                    return rollbackAndRespond(500, {message: 'Error checking analysis existence.', error: selectErr});
                }

                if (rows && rows.length > 0) {
                    const existingRow = rows[0];
                    return conn.commit((commitErr) => {
                        if (commitErr) {
                            console.error('Error committing transaction (exists path):', commitErr);
                            conn.release();
                            return res.status(500).json({message: 'Database commit error.', error: commitErr});
                        }
                        conn.release();
                        return res.status(200).json({
                            inserted: false,
                            exists: true,
                            data: buildAnalisisAdsResponse(existingRow),
                            row: existingRow
                        });
                    });
                }

                const insertSql = 'INSERT INTO Analisis_Ads (FK_LibraryID, Antiguedad, Contenido_Persuasivo, Problema, Publico, Nicho, Paises, Idioma, Angulo_Copy, Gancho_Video, Contenido_Video) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
                const insertParams = [
                    insertPayload.FK_LibraryID,
                    insertPayload.Antiguedad,
                    insertPayload.Contenido_Persuasivo,
                    insertPayload.Problema,
                    insertPayload.Publico,
                    insertPayload.Nicho,
                    insertPayload.Paises,
                    insertPayload.Idioma,
                    insertPayload.Angulo_Copy,
                    insertPayload.Gancho_Video,
                    insertPayload.Contenido_Video
                ];

                conn.query(insertSql, insertParams, (insertErr, insertResult) => {
                    if (insertErr) {
                        console.error('Error inserting Analisis_Ads:', insertErr);
                        return rollbackAndRespond(500, {message: 'Error inserting analysis.', error: insertErr});
                    }

                    const newId = insertResult && insertResult.insertId ? insertResult.insertId : null;
                    const fetchSql = newId
                        ? 'SELECT * FROM Analisis_Ads WHERE ID = ? LIMIT 1'
                        : 'SELECT * FROM Analisis_Ads WHERE FK_LibraryID = ? ORDER BY ID DESC LIMIT 1';
                    const fetchParams = newId ? [newId] : [libraryId];

                    conn.query(fetchSql, fetchParams, (fetchErr, fetchedRows) => {
                        if (fetchErr) {
                            console.error('Error fetching inserted Analisis_Ads:', fetchErr);
                            return rollbackAndRespond(500, {
                                message: 'Error fetching inserted analysis.',
                                error: fetchErr
                            });
                        }

                        const createdRow = fetchedRows && fetchedRows.length > 0 ? fetchedRows[0] : null;

                        return conn.commit((commitErr) => {
                            if (commitErr) {
                                console.error('Error committing transaction (insert path):', commitErr);
                                conn.release();
                                return res.status(500).json({message: 'Database commit error.', error: commitErr});
                            }
                            conn.release();
                            return res.status(201).json({
                                inserted: true,
                                exists: false,
                                id: newId,
                                data: createdRow ? buildAnalisisAdsResponse(createdRow) : buildAnalisisAdsResponse(insertPayload),
                                row: createdRow || insertPayload
                            });
                        });
                    });
                });
            });
        });
    });
});


// Endpoint to get records from the research view with filtering and pagination
router.get('/researchuser', async (req, res) => {
    const {
        UserPrompt,
        updated_date_min,
        updated_date_max,
        limit = 10,
        page = 1
    } = req.query;

    console.log('[GET /ads/researchuser] query params:', req.query);

    let whereClauses = [];
    let params = [];

    // Filter by UserPrompt
    if (UserPrompt) {
        whereClauses.push('UserPrompt LIKE ?');
        params.push(`%${UserPrompt}%`);
    }

    // Filter by researchedAt date range
    if (updated_date_min) {
        const fechahoraInicio = `${updated_date_min} 00:00:00`;
        whereClauses.push('researchedAt >= ?');
        params.push(fechahoraInicio);
    }
    if (updated_date_max) {
        const fechahoraFin = `${updated_date_max} 23:59:59`;
        whereClauses.push('researchedAt <= ?');
        params.push(fechahoraFin);
    }

    const whereString = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

    const countQuery = `SELECT COUNT(*) as totalItems
                        FROM view_research_ads_users ${whereString}`;
    console.log('[GET /ads/researchuser] WHERE:', whereString, 'PARAMS:', params);

    db.query(countQuery, params, (err, countResult) => {
        if (err) {
            console.error('Error fetching records count:', err);
            return res.status(500).json({message: 'Error fetching records from database.', error: err});
        }
        const totalItems = countResult[0].totalItems;
        const totalPages = Math.ceil(totalItems / limit);
        const offset = (page - 1) * limit;

        const dataQuery = `SELECT *
                           FROM view_research_ads_users ${whereString}
                           ORDER BY researchedAt DESC
                           LIMIT ? OFFSET ?`;
        const dataParams = [...params, parseInt(limit), parseInt(offset)];

        db.query(dataQuery, dataParams, (err, results) => {
            if (err) {
                console.error('Error fetching records:', err);
                return res.status(500).json({message: 'Error fetching records from database.', error: err});
            }

            const parsedResults = results.map(parseJsonFields);
            res.status(200).json({
                message: "Success",
                code: 200,
                error: false,
                status: "Ok",
                stage: "Ending",
                info: {
                    currentPage: parseInt(page),
                    totalPages: totalPages,
                    totalItems: totalItems,
                    hasNextPage: parseInt(page) < totalPages,
                    hasPrevPage: parseInt(page) > 1
                },
                after: parseInt(page) + 1,
                data: parsedResults
            });
        });
    });
});

// GET /ads/user-features
// Requires Authorization: Bearer <token>
router.get('/user-features', (req, res) => {
    const token = getBearerToken(req);
    if (!token) {
        return res.status(401).json({message: 'Missing or invalid bearer token.'});
    }

    const query = 'SELECT * FROM View_users_features WHERE Token = ? AND Active = 1';
    db.query(query, [token], (err, results) => {
        if (err) {
            console.error('Error fetching user features:', err);
            return res.status(500).json({message: 'Error fetching user features from database.', error: err});
        }
        if (!results || results.length === 0) {
            return res.status(404).json({message: 'No features found for this token.'});
        }
        return res.status(200).json({
            message: 'Success',
            code: 200,
            error: false,
            status: 'Ok',
            data: results
        });
    });
});

// GET /ads/user-feature-limits?featurePlanCode=<value>
// Requires Authorization: Bearer <token>
router.get('/user-feature-limits', (req, res) => {
    const token = getBearerToken(req);
    if (!token) {
        return res.status(401).json({message: 'Missing or invalid bearer token.'});
    }

    const featurePlanCode = req.query.featurePlanCode ? String(req.query.featurePlanCode).trim() : '';
    if (!featurePlanCode) {
        return res.status(400).json({message: 'Missing featurePlanCode query parameter.'});
    }

    const userQuery = 'SELECT FK_UserID FROM UserTokens WHERE Token = ?';
    db.query(userQuery, [token], (err, userResults) => {
        if (err) {
            console.error('Error fetching user token:', err);
            return res.status(500).json({message: 'Error fetching user token from database.', error: err});
        }
        if (!userResults || userResults.length === 0) {
            return res.status(404).json({message: 'Invalid token.'});
        }

        const userId = userResults[0].FK_UserID;
        const likeValue = `%${featurePlanCode}%`;
        const limitsQuery = 'SELECT * FROM View_user_features_limits WHERE FK_UserID = ? AND FeaturesPlanCode LIKE ? LIMIT 1';

        db.query(limitsQuery, [userId, likeValue], (err, results) => {
            if (err) {
                console.error('Error fetching user feature limits:', err);
                return res.status(500).json({message: 'Error fetching user feature limits from database.', error: err});
            }
            if (!results || results.length === 0) {
                return res.status(404).json({message: 'No feature limits found for this token and featurePlanCode.'});
            }

            const currentRow = results[0];
            const totalUsed = Number(currentRow.TotalUsed);
            const limite = Number(currentRow.Limite);

            if (Number.isNaN(totalUsed) || Number.isNaN(limite)) {
                return res.status(500).json({message: 'Invalid limit values from database.'});
            }

            if (totalUsed >= limite) {
                return res.status(403).json({message: 'Has llegado al límite'});
            }

            const updateQuery = 'UPDATE View_user_features_limits SET TotalUsed = TotalUsed + 1 WHERE FK_UserID = ? AND FeaturesPlanCode LIKE ? LIMIT 1';
            db.query(updateQuery, [userId, likeValue], (updateErr) => {
                if (updateErr) {
                    console.error('Error updating user feature limits:', updateErr);
                    return res.status(500).json({message: 'Error updating user feature limits.', error: updateErr});
                }

                db.query(limitsQuery, [userId, likeValue], (refetchErr, refetchResults) => {
                    if (refetchErr) {
                        console.error('Error refetching user feature limits:', refetchErr);
                        return res.status(500).json({
                            message: 'Error fetching user feature limits from database.',
                            error: refetchErr
                        });
                    }
                    if (!refetchResults || refetchResults.length === 0) {
                        return res.status(404).json({message: 'No feature limits found for this token and featurePlanCode.'});
                    }
                    return res.status(200).json({
                        message: 'Success',
                        code: 200,
                        error: false,
                        status: 'Ok',
                        data: refetchResults[0]
                    });
                });
            });
        });
    });
});

// Endpoint to get all records
router.get('/adsdomains', (req, res) => {
    const query = 'SELECT * FROM adsdomains';

    db.query(query, (err, results) => {
        if (err) {
            console.error('Error fetching records:', err);
            return res.status(500).json({message: 'Error fetching records from database.', error: err});
        }
        const parsedResults = results.map(parseJsonFields);
        res.status(200).json(parsedResults);
    });
});

// Endpoint to get a single record by LibraryID
router.get('/adsdomains/:libraryId', (req, res) => {
    const {libraryId} = req.params;
    const query = 'SELECT * FROM adsdomains WHERE LibraryID = ?';

    db.query(query, [libraryId], (err, results) => {
        if (err) {
            console.error('Error fetching record:', err);
            return res.status(500).json({message: 'Error fetching record from database.', error: err});
        }
        if (results.length === 0) {
            return res.status(404).json({message: 'Record not found.'});
        }
        const parsedResult = parseJsonFields(results[0]);
        res.status(200).json(parsedResult);
    });
});

// Endpoint to get a record by FK_LibraryID and token from adsdomains_usuarios
router.get('/adsdomains/:libraryId/:token', (req, res) => {
    const {libraryId, token} = req.params;

    // First, get FK_UserID from UserTokens using the token
    const userQuery = 'SELECT FK_UserID FROM UserTokens WHERE Token = ?';

    db.query(userQuery, [token], (err, userResults) => {
        if (err) {
            console.error('Error fetching user token:', err);
            return res.status(500).json({message: 'Error fetching user token from database.', error: err});
        }
        if (userResults.length === 0) {
            return res.status(404).json({message: 'Invalid token.'});
        }

        const userId = userResults[0].FK_UserID;
        const query = 'SELECT * FROM adsdomains_usuarios WHERE FK_LibraryID = ? AND FK_UserID = ?';

        db.query(query, [libraryId, userId], (err, results) => {
            if (err) {
                console.error('Error fetching record:', err);
                return res.status(500).json({message: 'Error fetching record from database.', error: err});
            }
            if (results.length === 0) {
                return res.status(404).json({message: 'Record not found.'});
            }
            // Note: adsdomains_usuarios likely doesn't need parseJsonFields since it's a link table
            // but keeping it for consistency if needed
            const parsedResult = parseJsonFields(results[0]);
            res.status(200).json(parsedResult);
        });
    });
});

// Endpoint: getadsdetailsforcardview?ids=LIBRARY_ID
// Fetch a single ad (by LibraryID) and return formatted structure for card view
// GET DETAILS FOR ADS SAVED USING THE EXTENSION
router.get('/getadssaveddetailsforcardview', (req, res) => {
    const {ids} = req.query; // expecting single LibraryID
    if (!ids) {
        return res.status(400).json({data: [], error: 'Missing ids parameter'});
    }
    const query = 'SELECT * FROM Anuncios WHERE LibraryID = ? LIMIT 1';
    db.query(query, [ids], (err, results) => {
        if (err) {
            console.error('Error fetching record for card view:', err);
            return res.status(500).json({data: [], error: 'Database error'});
        }
        if (results.length === 0) {
            return res.status(404).json({data: [], error: 'Not found'});
        }
        // Parse JSON fields for Anuncios table
        const row = results[0];
        console.log('[Anuncios keys]', Object.keys(row));
        console.log('[Anuncios page fields]', {
            pageName: row.pageName,
            PageName: row.PageName,
            pageID: row.pageID,
            PageID: row.PageID
        });

        // Parse JSON fields that exist in Anuncios table
        // These DB columns are not always valid JSON. Safely parse JSON when possible;
        // otherwise treat as plain text (or empty) without throwing.
        const parsedPlataformas = parseJsonArrayOrEmpty(row.Plataformas);
        const parsedAdDescription = parseTextOrJsonArray(row.AdDescription);
        const parsedAdTitle = parseTextOrJsonArray(row.AdTitle);

        // Derive fields according to required structure using Anuncios table fields
        const adTitle = Array.isArray(parsedAdTitle) && parsedAdTitle.length > 0 ? parsedAdTitle[0] : (row.pageName || '');
        const adDescription = Array.isArray(parsedAdDescription) && parsedAdDescription.length > 0 ? parsedAdDescription[0] : '';

        const formatted = {
            error: false,
            LibraryID: row.LibraryID || null,
            url_preview_creative: row.AdCreative || '', // Using AdCreative for preview
            startDate: row.startDate || null,
            endDate: row.endDate || null,
            cta_text: row.cta_text || '',
            cta_type: row.cta_type || '',
            __html: row.__html || '',
            link_url: row.ahref || '',
            title: adTitle || '',
            page_profile_uri: row.page_profile_uri || '',
            publisherPlatform: parsedPlataformas || [],
            pageName: row.pageName || '',
            pageID: row.pageID || null,
            URLCreative: row.AdCreative || '', // Using AdCreative field
            AdCreative: row.AdCreative || '',
            AdMedia: row.AdMedia || '',
            message: adDescription || '',
            profilePict: row.page_profile_picture_url || '', // Using available field
            page_profile_picture_url: row.page_profile_picture_url || '',
            Active: Boolean(row.Estatus), // Using Estatus field for Active status
            adsStatus: Boolean(row.Estatus),
            Estatus: row.Estatus || false,
            collectionCount: '', // Not available in Anuncios table
            collationId: null, // Not available in Anuncios table
            id: String(row.LibraryID || ''),
            fails: false
        };
        return res.status(200).json({data: [formatted], error: null});
    });
});

//GET DETAILS FOR ADS NOT SAVED
// Endpoint: getadsdetailsforcardview?ids=LIBRARY_ID
// Fetch a single ad (by LibraryID) and return formatted structure for card view
router.get('/getadsdetailsforcardview', (req, res) => {
    const {ids} = req.query; // expecting single LibraryID
    if (!ids) {
        return res.status(400).json({data: [], error: 'Missing ids parameter'});
    }
    const query = 'SELECT * FROM adsdomains WHERE LibraryID = ? LIMIT 1';
    db.query(query, [ids], (err, results) => {
        if (err) {
            console.error('Error fetching record for card view:', err);
            return res.status(500).json({data: [], error: 'Database error'});
        }
        if (results.length === 0) {
            return res.status(404).json({data: [], error: 'Not found'});
        }
        // Parse JSON fields
        const row = parseJsonFields(results[0]);
        // Derive fields according to required structure
        const adTitle = Array.isArray(row.AdTitle) && row.AdTitle.length > 0 ? row.AdTitle[0] : (row.pageName || '');
        const adDescription = Array.isArray(row.AdDescription) && row.AdDescription.length > 0 ? row.AdDescription[0] : '';
        const formatted = {
            error: false,
            LibraryID: row.LibraryID || null,
            url_preview_creative: row.url_preview_creative || '',
            startDate: row.startDate || null,
            endDate: row.endDate || null,
            cta_text: row.cta_text || '',
            cta_type: row.cta_type || '',
            __html: row.__html || '',
            link_url: row.ahref || '',
            title: adTitle || '',
            page_profile_uri: row.page_profile_uri || '',
            publisherPlatform: row.publisherPlatform || [],
            pageName: row.pageName || '',
            pageID: row.pageID || null,
            URLCreative: row.URLCreative || '',
            AdCreative: row.AdCreative || '',
            AdMedia: row.AdMedia || '',
            message: adDescription || '',
            profilePict: row.profilePict || '',
            page_profile_picture_url: row.page_profile_picture_url || '',
            Active: Boolean(row.Active),
            adsStatus: Boolean(row.Active),
            Estatus: row.Estatus || false,
            collectionCount: row.CollectionCount || '',
            collationId: row.CollationID || null,
            id: String(row.LibraryID || ''),
            fails: false
        };
        return res.status(200).json({data: [formatted], error: null});
    });
});

// Endpoint to insert records
router.post('/saveToMariaDB', (req, res) => {
    // Flexible body formats supported:
    // 1) Array of ads
    // 2) Single ad object
    // 3) { ads: [...], UserPrompt, token }
    let ads;
    if (Array.isArray(req.body)) {
        ads = req.body;
    } else if (req.body && Array.isArray(req.body.ads)) {
        ads = req.body.ads;
    } else if (req.body && req.body.ads && typeof req.body.ads === 'object') {
        ads = [req.body.ads];
    } else {
        ads = [req.body];
    }

    const userPrompt = (req.body && (req.body.UserPrompt || req.body.userPrompt)) ? String(req.body.UserPrompt || req.body.userPrompt).trim() : '';
    const token = (req.body && (req.body.token || req.body.Token)) ? String(req.body.token || req.body.Token).trim() : '';

    if (!ads || ads.length === 0) {
        return res.status(400).json({message: 'Request body must contain at least one ad object.'});
    }

    const adsInsertQuery = 'INSERT INTO adsdomains (cta_text, cta_type, __html, page_profile_uri, publisherPlatform, URLCreative, url_preview_creative, AdCreative, AdMedia, profilePict, page_profile_picture_url, Active, Estatus, CollectionCount, CollationID, startDate, endDate, LibraryID, ahref, pageName, pageID, AdDescription, AdTitle, age, gender, languages, countries, daysSincePublication, lazy_load, contains_details, domain, codeBelongs, keywords, duplicates) VALUES ?';

    const adsValues = ads.map(ad => [
        ad.cta_text,
        ad.cta_type,
        ad.__html,
        ad.page_profile_uri,
        JSON.stringify(ad.publisherPlatform),
        ad.URLCreative,
        ad.url_preview_creative,
        ad.AdCreative,
        ad.AdMedia,
        ad.profilePict,
        ad.page_profile_picture_url,
        ad.Active,
        ad.Estatus,
        ad.CollectionCount,
        ad.CollationID,
        ad.startDate,
        ad.endDate,
        ad.LibraryID,
        ad.ahref,
        ad.pageName,
        ad.pageID,
        JSON.stringify(ad.AdDescription),
        JSON.stringify(ad.AdTitle),
        JSON.stringify(ad.age),
        ad.gender,
        JSON.stringify(ad.languages),
        JSON.stringify(ad.countries),
        ad.daysSincePublication,
        ad.lazy_load,
        ad.contains_details,
        ad.domain,
        JSON.stringify(ad.codeBelongs),
        ad.keywords,
        ad.duplicates
    ]);

    db.query(adsInsertQuery, [adsValues], (err, insertResult) => {
        if (err) {
            console.error('Error inserting records:', err);
            return res.status(500).json({message: 'Error inserting records into database.', error: err});
        }

        // Base response (may be enriched below)
        const baseResponse = {
            message: 'Records inserted successfully.',
            affectedRows: insertResult.affectedRows
        };

        // Only attempt user linking if BOTH token & userPrompt provided and non-empty
        if (!token || !userPrompt) {
            return res.status(200).json({...baseResponse, userLink: {attempted: false}});
        }

        // Step 1: Get UserID from UserTokens by token (must be active & not expired)
        const userQuery = 'SELECT FK_UserID FROM UserTokens WHERE Token = ? AND Active = 1 AND (Expires IS NULL OR Expires > NOW()) LIMIT 1';
        db.query(userQuery, [token], (userErr, userRows) => {
            if (userErr) {
                console.error('Error looking up user token:', userErr);
                return res.status(200).json({
                    ...baseResponse,
                    userLink: {
                        attempted: true,
                        success: false,
                        userId: null,
                        rowsLinked: 0,
                        message: 'Token lookup failed.'
                    }
                });
            }
            if (!userRows || userRows.length === 0) {
                return res.status(200).json({
                    ...baseResponse,
                    userLink: {
                        attempted: true,
                        success: false,
                        userId: null,
                        rowsLinked: 0,
                        message: 'Invalid or expired token.'
                    }
                });
            }
            const userId = userRows[0].FK_UserID;
            // Step 2: Prepare link insert rows using LibraryID from each ad inserted
            const libraryIds = ads.map(a => a.LibraryID).filter(id => typeof id !== 'undefined' && id !== null && id !== '');
            if (libraryIds.length === 0) {
                return res.status(200).json({
                    ...baseResponse,
                    userLink: {
                        attempted: true,
                        success: false,
                        userId,
                        rowsLinked: 0,
                        message: 'No valid LibraryID values provided to link.'
                    }
                });
            }
            const linkValues = libraryIds.map(lib => [lib, userId, userPrompt, new Date()]);
            const linkInsert = 'INSERT INTO adsdomains_usuarios (FK_LibraryID, FK_UserID, UserPrompt, CreatedAt) VALUES ?';
            db.query(linkInsert, [linkValues], (linkErr, linkResult) => {
                if (linkErr) {
                    console.error('Error inserting adsdomains_usuarios records:', linkErr);
                    return res.status(200).json({
                        ...baseResponse,
                        userLink: {
                            attempted: true,
                            success: false,
                            userId,
                            rowsLinked: 0,
                            message: 'Failed to link ads to user.'
                        }
                    });
                }
                return res.status(200).json({
                    ...baseResponse,
                    userLink: {
                        attempted: true,
                        success: true,
                        userId,
                        rowsLinked: linkResult.affectedRows,
                        message: 'Ads linked to user successfully.'
                    }
                });
            });
        });
    });
});

// POST /linkaduser - Link an ad to a user via token
router.post('/linkaduser', (req, res) => {
    const {token, FK_LibraryID, UserPrompt} = req.body;

    if (!token || !FK_LibraryID) {
        return res.status(400).json({message: 'Token and FK_LibraryID are required.'});
    }

    // Step 1: Get FK_UserID from UserTokens using the token
    const userQuery = 'SELECT FK_UserID FROM UserTokens WHERE Token = ?';

    db.query(userQuery, [token], (err, userResults) => {
        if (err) {
            console.error('Error fetching user token:', err);
            return res.status(500).json({message: 'Error fetching user token from database.', error: err});
        }
        if (userResults.length === 0) {
            return res.status(404).json({message: 'Invalid token.'});
        }

        const userId = userResults[0].FK_UserID;

        // Step 2: Check if record already exists
        const checkQuery = 'SELECT * FROM adsdomains_usuarios WHERE FK_LibraryID = ? AND FK_UserID = ?';

        db.query(checkQuery, [FK_LibraryID, userId], (err, existingResults) => {
            if (err) {
                console.error('Error checking existing record:', err);
                return res.status(500).json({message: 'Error checking existing record.', error: err});
            }

            if (existingResults.length > 0) {
                return res.status(409).json({
                    message: 'Record already exists.',
                    existingRecord: existingResults[0]
                });
            }

            // Step 3: Insert new record if it doesn't exist
            const insertQuery = 'INSERT INTO adsdomains_usuarios (FK_LibraryID, FK_UserID, UserPrompt, CreatedAt) VALUES (?, ?, ?, ?)';
            const insertValues = [FK_LibraryID, userId, UserPrompt || null, new Date()];

            db.query(insertQuery, insertValues, (err, insertResult) => {
                if (err) {
                    console.error('Error inserting record:', err);
                    return res.status(500).json({message: 'Error inserting record into database.', error: err});
                }

                res.status(201).json({
                    message: 'Ad linked to user successfully.',
                    id: insertResult.insertId,
                    FK_LibraryID: FK_LibraryID,
                    FK_UserID: userId,
                    UserPrompt: UserPrompt || null
                });
            });
        });
    });
});

// Endpoint to delete a record
router.delete('/adsdomains/:id', (req, res) => {
    const {id} = req.params;
    const query = 'DELETE FROM adsdomains WHERE id = ?';

    db.query(query, [id], (err, result) => {
        if (err) {
            console.error('Error deleting record:', err);
            return res.status(500).json({message: 'Error deleting record from database.', error: err});
        }
        if (result.affectedRows === 0) {
            return res.status(404).json({message: 'Record not found.'});
        }
        res.status(200).json({message: 'Record deleted successfully.'});
    });
});

// Endpoint to update a record
router.put('/adsdomains/:id', (req, res) => {
    const {id} = req.params;
    const ad = req.body;

    // Stringify JSON fields
    const fieldsToUpdate = {};
    for (const key in ad) {
        if (Object.hasOwnProperty.call(ad, key)) {
            const value = ad[key];
            if (['publisherPlatform', 'AdDescription', 'AdTitle', 'age', 'languages', 'countries', 'codeBelongs'].includes(key) && typeof value !== 'string') {
                fieldsToUpdate[key] = JSON.stringify(value);
            } else {
                fieldsToUpdate[key] = value;
            }
        }
    }


    const query = 'UPDATE adsdomains SET ? WHERE id = ?';

    db.query(query, [fieldsToUpdate, id], (err, result) => {
        if (err) {
            console.error('Error updating record:', err);
            return res.status(500).json({message: 'Error updating record in database.', error: err});
        }
        if (result.affectedRows === 0) {
            return res.status(404).json({message: 'Record not found.'});
        }
        res.status(200).json({message: 'Record updated successfully.'});
    });
});

// --- S3 Links Endpoints ---

// POST /s3-links - Create a new S3 link record
router.post('/s3-links', (req, res) => {
    const {LibraryID, Location, _key, Bucket} = req.body;

    if (!LibraryID || !_key) {
        return res.status(400).json({message: 'LibraryID and _key are required.'});
    }

    const query = 'INSERT INTO lnk_ads_s3_bucket (LibraryID, Location, _key, Bucket) VALUES (?, ?, ?, ?)';
    const values = [LibraryID, Location, _key, Bucket];

    db.query(query, values, (err, result) => {
        if (err) {
            console.error('Error creating S3 link record:', err);
            return res.status(500).json({message: 'Error creating S3 link record.', error: err});
        }
        res.status(201).json({message: 'S3 link record created successfully.', id: result.insertId});
    });
});

// GET /s3-links/:libraryId - Get an S3 link record by LibraryID
router.get('/s3-links/:libraryId', (req, res) => {
    const {libraryId} = req.params;
    const query = 'SELECT * FROM lnk_ads_s3_bucket WHERE LibraryID = ?';

    db.query(query, [libraryId], (err, results) => {
        if (err) {
            console.error('Error fetching S3 link record:', err);
            return res.status(500).json({message: 'Error fetching S3 link record.', error: err});
        }
        if (results.length === 0) {
            return res.status(404).json({message: 'S3 link record not found.'});
        }
        res.status(200).json(results[0]);
    });
});

// PUT /s3-links/:libraryId - Update an S3 link record by LibraryID
router.put('/s3-links/:libraryId', (req, res) => {
    const {libraryId} = req.params;
    const fieldsToUpdate = req.body;

    const query = 'UPDATE lnk_ads_s3_bucket SET ? WHERE LibraryID = ?';

    db.query(query, [fieldsToUpdate, libraryId], (err, result) => {
        if (err) {
            console.error('Error updating S3 link record:', err);
            return res.status(500).json({message: 'Error updating S3 link record.', error: err});
        }
        if (result.affectedRows === 0) {
            return res.status(404).json({message: 'S3 link record not found.'});
        }
        res.status(200).json({message: 'S3 link record updated successfully.'});
    });
});

// DELETE /s3-links/:libraryId - Delete an S3 link record by LibraryID
router.delete('/s3-links/:libraryId', (req, res) => {
    const {libraryId} = req.params;
    const query = 'DELETE FROM lnk_ads_s3_bucket WHERE LibraryID = ?';

    db.query(query, [libraryId], (err, result) => {
        if (err) {
            console.error('Error deleting S3 link record:', err);
            return res.status(500).json({message: 'Error deleting S3 link record.', error: err});
        }
        if (result.affectedRows === 0) {
            return res.status(404).json({message: 'S3 link record not found.'});
        }
        res.status(200).json({message: 'S3 link record deleted successfully.'});
    });
});

// --- Prompts Endpoint ---

// GET /prompts - Get all prompts grouped by category
router.get('/prompts', (req, res) => {
    const query = 'SELECT categoria, prompt FROM prompts ORDER BY categoria';

    db.query(query, (err, results) => {
        if (err) {
            console.error('Error fetching prompts:', err);
            return res.status(500).json({message: 'Error fetching prompts from database.', error: err});
        }

        // Procesa los resultados para agruparlos por categoría
        const groupedPrompts = results.reduce((acc, row) => {
            const {categoria, prompt} = row;
            if (!acc[categoria]) {
                acc[categoria] = [];
            }
            acc[categoria].push(prompt);
            return acc;
        }, {});

        res.status(200).json(groupedPrompts);
    });
});


// --- User Prompts (capture user submitted prompts) ---
// POST /userprompts
// Body: { "userprompt": "text the user entered" }
router.post('/userprompts', (req, res) => {
    const {userprompt} = req.body;

    if (!userprompt || typeof userprompt !== 'string' || !userprompt.trim()) {
        return res.status(400).json({message: 'Field userprompt (non-empty string) is required.'});
    }

    const query = 'INSERT INTO userprompts (prompt, succeded) VALUES (?, ?)';
    const values = [userprompt.trim(), 0]; // succeded defaults to false (0)

    db.query(query, values, (err, result) => {
        if (err) {
            console.error('Error saving user prompt:', err);
            return res.status(500).json({message: 'Error saving user prompt.', error: err});
        }
        res.status(201).json({
            message: 'User prompt saved.',
            id: result.insertId,
            prompt: userprompt.trim(),
            succeded: false
        });
    });
});

// PUT /userprompts/:id/succeded  -> mark succeded = true
router.put('/userprompts/:id/succeded', (req, res) => {
    const {id} = req.params;
    if (!/^\d+$/.test(id)) {
        return res.status(400).json({message: 'Invalid id.'});
    }
    const query = 'UPDATE userprompts SET succeded = 1 WHERE id = ?';
    db.query(query, [id], (err, result) => {
        if (err) {
            console.error('Error updating user prompt:', err);
            return res.status(500).json({message: 'Error updating user prompt.', error: err});
        }
        if (result.affectedRows === 0) {
            return res.status(404).json({message: 'User prompt not found.'});
        }
        res.status(200).json({message: 'User prompt marked as succeded.', id: Number(id), succeded: true});
    });
});

router.get('/status', (req, res) => {
    res.status(200).json({app: 'ads', status: 'ok', port: port});
})

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});

