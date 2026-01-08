const fs = require('fs');
const path = require('path');
const axios = require('axios');

// 导入 p-limit，支持不同版本的兼容性
let pLimit;
try {
  pLimit = require('p-limit').default || require('p-limit');
} catch (error) {
  // 如果 p-limit 不可用，使用简单的并发控制
  pLimit = (concurrency) => {
    let running = 0;
    const queue = [];

    const next = () => {
      if (running < concurrency && queue.length > 0) {
        running++;
        const { fn, resolve, reject } = queue.shift();
        fn().then(resolve).catch(reject).finally(() => {
          running--;
          next();
        });
      }
    };

    return (fn) => new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      next();
    });
  };
}

// 配置对象 - 直接修改这里
const config = {
  inputDir: './data/input',        // 输入目录路径
  outputDir: './data/output',      // 输出目录路径
  recursive: true,                 // 是否递归扫描子目录
  serviceProvider: 'baidu',        // 地图服务提供商：'baidu' 或 'amap'
  apiKey: 'YOUR_API_KEY',          // API 密钥（百度 AK 或高德 Key）
  sourceCoordType: 'wgs84',        // 源坐标系：'wgs84', 'gcj02', 'bd09'
  targetCoordType: 'bd09',         // 目标坐标系：'wgs84', 'gcj02', 'bd09'
  cacheFile: './cache/coord-cache.json',  // 缓存文件路径
  enableCache: true,               // 是否启用缓存
  maxConcurrency: 2,               // 最大并发请求数
  batchSize: 100,                  // 单次批量大小（百度最多100，高德最多40）
  dailyQuota: 5000,                // 每日配额限制
  retryTimes: 3,                   // 失败重试次数
  retryDelay: 1000                 // 重试延迟（毫秒，采用指数退避策略）
};

// 全局缓存变量
let globalCache = new Map();
let isCacheSaving = false; // 防止重复保存的标志

// 工具函数
function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function log(message, level = 'INFO') {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${level}: ${message}`);
}

function formatDuration(ms) {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) {
    return `${hours}小时 ${minutes % 60}分钟 ${seconds % 60}秒`;
  } else if (minutes > 0) {
    return `${minutes}分钟 ${seconds % 60}秒`;
  } else {
    return `${seconds}秒`;
  }
}

// 缓存管理函数
function getCacheKey(lon, lat) {
  return `${lon},${lat}`;
}

function loadCache(cacheFile) {
  try {
    if (fs.existsSync(cacheFile)) {
      const data = fs.readFileSync(cacheFile, 'utf8');
      const json = JSON.parse(data);
      const cache = new Map();

      for (const [key, value] of Object.entries(json)) {
        cache.set(key, value);
      }

      log(`加载缓存: ${cache.size} 条记录`);
      return cache;
    }
  } catch (error) {
    log(`加载缓存失败: ${error.message}`, 'WARN');
  }

  return new Map();
}

function saveCache(cacheFile, cacheMap) {
  if (isCacheSaving) return; // 防止并发保存

  try {
    isCacheSaving = true;
    ensureDir(path.dirname(cacheFile));

    const json = {};
    for (const [key, value] of cacheMap.entries()) {
      json[key] = value;
    }

    fs.writeFileSync(cacheFile, JSON.stringify(json, null, 2), 'utf8');
    log(`保存缓存: ${cacheMap.size} 条记录`);
  } catch (error) {
    log(`保存缓存失败: ${error.message}`, 'ERROR');
  } finally {
    isCacheSaving = false;
  }
}

// 文件扫描函数
function scanGeoJSONFiles(inputDir, outputDir, recursive) {
  const files = [];

  function scan(dir, relativePath = '') {
    if (!fs.existsSync(dir)) {
      throw new Error(`输入目录不存在: ${dir}`);
    }

    const items = fs.readdirSync(dir);

    for (const item of items) {
      const fullPath = path.join(dir, item);
      const stat = fs.statSync(fullPath);
      const relPath = path.join(relativePath, item);

      if (stat.isDirectory() && recursive) {
        scan(fullPath, relPath);
      } else if (stat.isFile() && item.toLowerCase().endsWith('.json')) {
        const inputFile = fullPath;
        const outputFile = path.join(outputDir, relPath);
        files.push({ inputFile, outputFile });
      }
    }
  }

  scan(inputDir);
  return files;
}

// GeoJSON 处理函数
function readGeoJSON(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(content);
}

function extractCoordinates(geojson) {
  const coordinates = [];

  function extract(obj, path = []) {
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        extract(obj[i], [...path, i]);
      }
    } else if (typeof obj === 'object' && obj !== null) {
      if (obj.type === 'FeatureCollection' && obj.features) {
        extract(obj.features, [...path, 'features']);
      } else if (obj.type === 'Feature' && obj.geometry) {
        extract(obj.geometry, [...path, 'geometry']);
      } else if (obj.type === 'GeometryCollection' && obj.geometries) {
        extract(obj.geometries, [...path, 'geometries']);
      } else if (obj.coordinates) {
        // 提取坐标
        extractCoords(obj.coordinates, [...path, 'coordinates']);
      } else {
        // 递归处理其他属性
        for (const key in obj) {
          if (obj.hasOwnProperty(key) && key !== 'properties' && key !== 'id') {
            extract(obj[key], [...path, key]);
          }
        }
      }
    }
  }

  function extractCoords(coords, path) {
    if (Array.isArray(coords)) {
      if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
        // [lon, lat] 格式
        coordinates.push({
          coordinate: [coords[0], coords[1]],
          path: path
        });
      } else {
        // 多层数组，递归处理
        for (let i = 0; i < coords.length; i++) {
          extractCoords(coords[i], [...path, i]);
        }
      }
    }
  }

  extract(geojson);
  return coordinates;
}

function applyConvertedCoordinates(geojson, coordinateRefs, results) {
  const resultMap = new Map();
  results.forEach(result => {
    resultMap.set(getCacheKey(result.original[0], result.original[1]), result.converted);
  });

  coordinateRefs.forEach(ref => {
    const key = getCacheKey(ref.coordinate[0], ref.coordinate[1]);
    const newCoord = resultMap.get(key);

    if (newCoord) {
      let current = geojson;
      for (let i = 0; i < ref.path.length - 1; i++) {
        current = current[ref.path[i]];
      }
      current[ref.path[ref.path.length - 1]] = newCoord;
    }
  });

  return geojson;
}

function writeGeoJSON(filePath, geojson) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(geojson), 'utf8');
}

// API 调用函数
async function convertBatchBaidu(coordinates, apiKey, from, to) {
  const coordStrings = coordinates.map(coord => `${coord[0]},${coord[1]}`).join(';');

  // 根据源坐标系和目标坐标系确定 model 参数
  let model;
  if (from === 'wgs84' && to === 'bd09') {
    model = 2; // gps to bd09ll
  } else if (from === 'gcj02' && to === 'bd09') {
    model = 1; // amap/tencent to bd09ll
  } else if (from === 'bd09' && to === 'gcj02') {
    model = 5; // bd09ll to amap/tencent
  } else if (from === 'bd09' && to === 'wgs84') {
    // 注意：文档明确说明不支持将任何类型的坐标转换为GPS坐标
    throw new Error('不支持将坐标转换为WGS84(GPS)坐标系');
  } else {
    throw new Error(`不支持的坐标系转换: ${from} -> ${to}`);
  }

  const params = {
    coords: coordStrings,
    model: model,
    ak: apiKey,
    output: 'json'
  };

  const response = await axios.get('https://api.map.baidu.com/geoconv/v2/', { params });

  if (response.data.status !== 0) {
    let errorMsg = `百度API错误: ${response.data.status}`;
    switch (response.data.status) {
      case 1:
        errorMsg += ' - 内部错误';
        break;
      case 4:
        errorMsg += ' - 转换失败（不支持转换为GPS坐标）';
        break;
      case 24:
        errorMsg += ' - coords格式非法';
        break;
      case 25:
        errorMsg += ' - coords个数非法，超过限制';
        break;
      case 26:
        errorMsg += ' - 参数错误';
        break;
      case 29:
        errorMsg += ' - model参数错误';
        break;
      default:
        errorMsg += ' - 未知错误';
    }
    throw new Error(errorMsg);
  }

  return response.data.result.map(item => [item.x, item.y]);
}

async function convertBatchAmap(coordinates, apiKey, from, to) {
  // 高德地图坐标转换最多支持40个坐标点
  if (coordinates.length > 40) {
    throw new Error('高德API单次最多支持40个坐标点');
  }

  // 构造坐标字符串：经度,纬度|经度,纬度|...
  const coordStrings = coordinates.map(coord => `${coord[0]},${coord[1]}`).join('|');

  // 确定原坐标系参数
  let coordsys;
  if (from === 'wgs84') {
    coordsys = 'gps';
  } else if (from === 'bd09') {
    coordsys = 'baidu';
  } else if (from === 'gcj02') {
    coordsys = 'autonavi'; // 高德坐标，不进行转换
  } else {
    throw new Error(`高德API不支持的源坐标系: ${from}`);
  }

  // 如果源坐标系就是目标坐标系（gcj02），直接返回原坐标
  if ((from === 'gcj02' && to === 'gcj02') ||
      (from === 'wgs84' && to === 'wgs84') ||
      (from === 'bd09' && to === 'bd09')) {
    return coordinates.map(coord => [coord[0], coord[1]]);
  }

  // 如果需要转换为gcj02坐标
  if (to !== 'gcj02') {
    throw new Error(`高德API只能转换为GCJ02坐标系，当前目标坐标系: ${to}`);
  }

  const params = {
    locations: coordStrings,
    coordsys: coordsys,
    key: apiKey,
    output: 'json'
  };

  const response = await axios.get('https://restapi.amap.com/v3/assistant/coordinate/convert', { params });

  if (response.data.status !== '1') {
    let errorMsg = `高德API错误: ${response.data.infocode || response.data.status}`;
    if (response.data.info) {
      errorMsg += ` - ${response.data.info}`;
    }
    throw new Error(errorMsg);
  }

  // 解析返回的坐标字符串：经度,纬度;经度,纬度;...
  const locations = response.data.locations;
  if (!locations) {
    throw new Error('高德API返回数据中没有坐标信息');
  }

  const coordPairs = locations.split(';');
  if (coordPairs.length !== coordinates.length) {
    throw new Error(`高德API返回坐标数量不匹配: 期望 ${coordinates.length} 个，实际 ${coordPairs.length} 个`);
  }

  return coordPairs.map(pair => {
    const [lon, lat] = pair.split(',').map(Number);
    if (isNaN(lon) || isNaN(lat)) {
      throw new Error(`高德API返回坐标格式错误: ${pair}`);
    }
    return [lon, lat];
  });
}

async function convertCoordinates(coordinates, config, cache, quota, saveCacheFn) {
  if (coordinates.length === 0) return [];

  // 过滤缓存
  const uncached = [];
  const results = [];

  for (const coord of coordinates) {
    const key = getCacheKey(coord[0], coord[1]);
    const cached = cache.get(key);

    if (cached && config.enableCache) {
      results.push({
        original: coord,
        converted: cached,
        fromCache: true
      });
    } else {
      uncached.push(coord);
    }
  }

  if (uncached.length === 0) return results;

  // 分批处理
  const batches = [];
  for (let i = 0; i < uncached.length; i += config.batchSize) {
    batches.push(uncached.slice(i, i + config.batchSize));
  }

  const limit = pLimit(config.maxConcurrency);

  for (const batch of batches) {
    await limit(async () => {
      // 检查配额
      if (!checkQuota(quota, 1, config.dailyQuota)) {
        throw new Error('配额不足，无法继续转换');
      }

      let lastError;
      for (let retry = 0; retry <= config.retryTimes; retry++) {
        try {
          let convertedCoords;
          if (config.serviceProvider === 'baidu') {
            convertedCoords = await convertBatchBaidu(batch, config.apiKey, config.sourceCoordType, config.targetCoordType);
          } else if (config.serviceProvider === 'amap') {
            convertedCoords = await convertBatchAmap(batch, config.apiKey, config.sourceCoordType, config.targetCoordType);
          } else {
            throw new Error(`不支持的服务提供商: ${config.serviceProvider}`);
          }

          // 更新缓存
          for (let i = 0; i < batch.length; i++) {
            const key = getCacheKey(batch[i][0], batch[i][1]);
            cache.set(key, convertedCoords[i]);
            results.push({
              original: batch[i],
              converted: convertedCoords[i],
              fromCache: false
            });
          }

          // 更新配额
          recordQuota(quota, 1);

          // 每个批次完成后立即保存缓存
          saveCacheFn();

          return;
        } catch (error) {
          lastError = error;
          if (retry < config.retryTimes) {
            log(`批次转换失败，重试 ${retry + 1}/${config.retryTimes}: ${error.message}`, 'WARN');
            await new Promise(resolve => setTimeout(resolve, config.retryDelay * Math.pow(2, retry)));
          }
        }
      }

      throw lastError;
    });
  }

  return results;
}

// 配额管理函数
function loadQuota(quotaFile) {
  try {
    if (fs.existsSync(quotaFile)) {
      const data = fs.readFileSync(quotaFile, 'utf8');
      return JSON.parse(data);
    }
  } catch (error) {
    log(`加载配额记录失败: ${error.message}`, 'WARN');
  }

  const today = new Date().toISOString().split('T')[0];
  return {
    date: today,
    count: 0,
    limit: config.dailyQuota,
    history: []
  };
}

function saveQuota(quotaFile, quotaData) {
  try {
    ensureDir(path.dirname(quotaFile));
    fs.writeFileSync(quotaFile, JSON.stringify(quotaData, null, 2), 'utf8');
  } catch (error) {
    log(`保存配额记录失败: ${error.message}`, 'ERROR');
  }
}

function checkQuota(quotaData, batchCount, dailyLimit) {
  const today = new Date().toISOString().split('T')[0];

  // 检查是否是新的一天
  if (quotaData.date !== today) {
    quotaData.date = today;
    quotaData.count = 0;
    quotaData.history = [];
  }

  return quotaData.count + batchCount <= dailyLimit;
}

function recordQuota(quotaData, batchCount) {
  quotaData.count += batchCount;
  quotaData.history.push({
    timestamp: new Date().toISOString(),
    batchSize: batchCount,
    status: 'success'
  });
}

// 主流程函数
async function main() {
  const startTime = Date.now();

  try {
    log('开始批量坐标转换');

    // 验证配置
    if (!config.inputDir || !config.outputDir || !config.apiKey) {
      throw new Error('配置不完整：请设置 inputDir、outputDir 和 apiKey');
    }

    if (config.apiKey === 'YOUR_API_KEY') {
      throw new Error('请在配置中设置有效的 API 密钥');
    }

    // 创建输出目录
    ensureDir(config.outputDir);
    ensureDir(path.dirname(config.cacheFile));

    // 加载缓存和配额
    globalCache = loadCache(config.cacheFile);
    const quotaFile = './cache/quota.json';
    const quota = loadQuota(quotaFile);

    // 注册异常处理钩子
    const saveCacheFn = () => saveCache(config.cacheFile, globalCache);

    process.on('SIGINT', () => {
      log('收到中断信号，正在保存缓存...', 'WARN');
      saveCacheFn();
      saveQuota(quotaFile, quota);
      process.exit(0);
    });

    process.on('SIGTERM', () => {
      log('收到终止信号，正在保存缓存...', 'WARN');
      saveCacheFn();
      saveQuota(quotaFile, quota);
      process.exit(0);
    });

    process.on('uncaughtException', (error) => {
      log(`未捕获异常: ${error.message}`, 'ERROR');
      saveCacheFn();
      saveQuota(quotaFile, quota);
      process.exit(1);
    });

    process.on('unhandledRejection', (reason) => {
      log(`未处理的Promise拒绝: ${reason}`, 'ERROR');
      saveCacheFn();
      saveQuota(quotaFile, quota);
      process.exit(1);
    });

    // 扫描文件
    const files = scanGeoJSONFiles(config.inputDir, config.outputDir, config.recursive);
    log(`发现 ${files.length} 个 GeoJSON 文件`);

    if (files.length === 0) {
      log('没有找到需要转换的文件');
      return;
    }

    // 第一阶段：扫描所有文件，收集所有坐标和缓存信息
    log('第一阶段：扫描文件并收集坐标...');

    const fileData = [];
    let totalCoordinates = 0;
    let totalCachedCoordinates = 0;
    let skippedFiles = 0;

    for (let i = 0; i < files.length; i++) {
      const { inputFile, outputFile } = files[i];

      try {
        log(`扫描文件 [${i + 1}/${files.length}]: ${path.relative(config.inputDir, inputFile)}`);

        // 检查输出文件是否已存在，如果存在则跳过
        if (fs.existsSync(outputFile)) {
          log('  输出文件已存在，跳过');
          skippedFiles++;
          continue;
        }

        // 读取和解析
        const geojson = readGeoJSON(inputFile);
        const coordinateRefs = extractCoordinates(geojson);

        if (coordinateRefs.length === 0) {
          log('  无坐标数据，跳过');
          continue;
        }

        // 检查缓存状态
        const coordinates = coordinateRefs.map(ref => ref.coordinate);
        const cachedResults = [];
        const uncachedCoords = [];

        for (const coord of coordinates) {
          const key = getCacheKey(coord[0], coord[1]);
          const cached = globalCache.get(key);

          if (cached && config.enableCache) {
            cachedResults.push({
              original: coord,
              converted: cached,
              fromCache: true
            });
          } else {
            uncachedCoords.push(coord);
          }
        }

        totalCoordinates += coordinates.length;
        totalCachedCoordinates += cachedResults.length;

        fileData.push({
          inputFile,
          outputFile,
          geojson,
          coordinateRefs,
          cachedResults,
          uncachedCoords,
          coordinates
        });

        log(`  坐标: ${coordinates.length} 个 | 缓存命中: ${cachedResults.length} 个 | 需转换: ${uncachedCoords.length} 个`);

      } catch (error) {
        log(`文件扫描失败 ${path.relative(config.inputDir, inputFile)}: ${error.message}`, 'ERROR');
      }
    }

    // 收集所有需要转换的坐标
    const allUncachedCoords = fileData.flatMap(fd => fd.uncachedCoords);

    log(`扫描完成: 发现 ${files.length} 个文件 | 跳过 ${skippedFiles} 个 | 处理 ${fileData.length} 个`);
    log(`总坐标 ${totalCoordinates} 个 | 缓存命中 ${totalCachedCoordinates} 个 | 需API转换 ${allUncachedCoords.length} 个`);

    // 第二阶段：批量转换所有未缓存坐标
    let apiResults = [];
    let apiCallCount = 0;

    if (allUncachedCoords.length > 0) {
      log('第二阶段：批量转换坐标...');

      // 分批处理所有未缓存坐标
      const batches = [];
      for (let i = 0; i < allUncachedCoords.length; i += config.batchSize) {
        batches.push(allUncachedCoords.slice(i, i + config.batchSize));
      }

      const limit = pLimit(config.maxConcurrency);

      for (const batch of batches) {
        await limit(async () => {
          // 检查配额
          if (!checkQuota(quota, 1, config.dailyQuota)) {
            throw new Error('配额不足，无法继续转换');
          }

          let lastError;
          for (let retry = 0; retry <= config.retryTimes; retry++) {
            try {
              let convertedCoords;
              if (config.serviceProvider === 'baidu') {
                convertedCoords = await convertBatchBaidu(batch, config.apiKey, config.sourceCoordType, config.targetCoordType);
              } else if (config.serviceProvider === 'amap') {
                convertedCoords = await convertBatchAmap(batch, config.apiKey, config.sourceCoordType, config.targetCoordType);
              } else {
                throw new Error(`不支持的服务提供商: ${config.serviceProvider}`);
              }

              // 更新缓存
              for (let i = 0; i < batch.length; i++) {
                const key = getCacheKey(batch[i][0], batch[i][1]);
                globalCache.set(key, convertedCoords[i]);
                apiResults.push({
                  original: batch[i],
                  converted: convertedCoords[i],
                  fromCache: false
                });
              }

              // 更新配额
              recordQuota(quota, 1);
              apiCallCount++;

              // 每个批次完成后立即保存缓存
              saveCacheFn();

              log(`批次转换完成: ${batch.length} 个坐标 | API调用 ${apiCallCount} 次`);
              return;
            } catch (error) {
              lastError = error;
              if (retry < config.retryTimes) {
                log(`批次转换失败，重试 ${retry + 1}/${config.retryTimes}: ${error.message}`, 'WARN');
                await new Promise(resolve => setTimeout(resolve, config.retryDelay * Math.pow(2, retry)));
              }
            }
          }

          throw lastError;
        });
      }
    }

    // 第三阶段：应用转换结果并保存文件
    log('第三阶段：应用转换结果并保存文件...');

    let successFiles = 0;
    let failedFiles = 0;

    for (let i = 0; i < fileData.length; i++) {
      const fileInfo = fileData[i];
      const fileStartTime = Date.now();

      try {
        log(`保存文件 [${i + 1}/${fileData.length}]: ${path.relative(config.inputDir, fileInfo.inputFile)}`);

        // 合并缓存结果和API结果
        const allResults = [...fileInfo.cachedResults];

        // 从API结果中找到属于此文件的坐标
        for (const coord of fileInfo.uncachedCoords) {
          const key = getCacheKey(coord[0], coord[1]);
          const apiResult = apiResults.find(r => getCacheKey(r.original[0], r.original[1]) === key);
          if (apiResult) {
            allResults.push(apiResult);
          }
        }

        // 应用转换结果
        const convertedGeojson = applyConvertedCoordinates(fileInfo.geojson, fileInfo.coordinateRefs, allResults);

        // 保存文件
        writeGeoJSON(fileInfo.outputFile, convertedGeojson);

        const fileDuration = Date.now() - fileStartTime;
        log(`  文件保存完成: 耗时 ${formatDuration(fileDuration)}`);

        successFiles++;

      } catch (error) {
        log(`文件保存失败 ${path.relative(config.inputDir, fileInfo.inputFile)}: ${error.message}`, 'ERROR');
        failedFiles++;
      }
    }

    // 输出统计报告
    const totalDuration = Date.now() - startTime;
    const apiConvertedCoordinates = apiResults.length;
    const totalProcessedFiles = successFiles + failedFiles;

    log('----------------------------------------');
    log('批量转换完成!');
    log(`总文件数: ${files.length} 个 | 跳过: ${skippedFiles} 个 | 处理: ${totalProcessedFiles} 个`);
    log(`成功: ${successFiles} 个 | 失败: ${failedFiles} 个`);
    log(`总坐标数: ${totalCoordinates} 个 | 缓存命中: ${totalCachedCoordinates} 个 | API转换: ${apiConvertedCoordinates} 个`);
    log(`API调用: ${apiCallCount} 次 | 剩余配额: ${config.dailyQuota - quota.count} 次`);
    log(`总耗时: ${formatDuration(totalDuration)}`);

  } catch (error) {
    log(`程序执行失败: ${error.message}`, 'ERROR');
    process.exit(1);
  }
}

// 启动程序
if (require.main === module) {
  main().catch(error => {
    log(`程序异常退出: ${error.message}`, 'ERROR');
    process.exit(1);
  });
}

module.exports = { main, config }; 
