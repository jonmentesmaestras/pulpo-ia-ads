const NodeCache = require('node-cache');

// 300 es igual a 5 minutos
// 600 es igual a 10 minutos
// 3600 es igual a 1 hora
// 7200 es igual a 2 horas
// 18000 es igual a 5 horas
const adsCache = new NodeCache({stdTTL: 7200});

const cacheMiddleware = (req, res, next) => {

    // Verificamos si está el flag que indica que se debe recargar la data o ignorar el caché.
    const forceReload = req.query.reload === 'true';
    // Obtener el query
    const queryParams = {...req.query};
    // Eliminar el parámetro reload
    delete queryParams.reload;
    // La llave que identifica el caché, es la url sin el parámetro 'reload'
    const queryString = new URLSearchParams(queryParams).toString();
    // Crear la llave
    const key = `${req.path}?${queryString}`; // Ejemplo: /{endpoint}?limit=10&page=1

    if (!forceReload) {
        const cachedResponse = adsCache.get(key);
        if (cachedResponse) {
            console.log('Responser desde el cached');
            return res.status(200).json(cachedResponse);
        }
    } else {
        console.log('Cargando nuevos datos desde la base de datos');
    }

    /*  OJO JON, AQUÍ SUCEDE LA MAGIA   */
    // Interceptamos la respuesta para guardarla antes de enviarla al front
    const originalJson = res.json.bind(res);
    res.json = (body) => {
        if (res.statusCode === 200 && body && !body.error) {
            adsCache.set(key, body);
        }
        return originalJson(body);
    };
    next();
};

module.exports = cacheMiddleware;