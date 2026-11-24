/* Byte-preserving local record restore. No credentials or arbitrary paths. */
(function(root) {
'use strict';
const allowed = new Set(['VA_Tracks_Output.csv','VA_Album_Date_Cache.json','VA_Scan_Metrics.json','VA_Scan_Tuning_Report.txt']);
function validateManifest(manifest) {
  if(manifest.schema!==1||!Array.isArray(manifest.files)||!manifest.files.length)throw new Error('回传清单格式不正确');
  const seen=new Set();
  for(const file of manifest.files) {
    if(!(allowed.has(file.path)||/^VA_Artist_Caches\/artists_[0-9a-f]{16}\.json$/.test(file.path))||seen.has(file.path)||!Number.isSafeInteger(file.size)||file.size<0||!/^[0-9a-f]{64}$/.test(file.sha256))throw new Error('回传清单包含不允许的文件');
    seen.add(file.path);
  }
  return manifest;
}
async function hash(blob) {return [...new Uint8Array(await crypto.subtle.digest('SHA-256',await blob.arrayBuffer()))].map(v=>v.toString(16).padStart(2,'0')).join('');}
async function directoryFor(root,path,create=false) {const parts=path.split('/');parts.pop();for(const name of parts)root=await root.getDirectoryHandle(name,{create});return root;}
async function existing(root,path) {
  try{return await (await directoryFor(root,path)).getFileHandle(path.split('/').pop());}
  catch(e){if(e.name==='NotFoundError')return null;throw e;}
}
async function write(root,path,blob) {
  const dir=await directoryFor(root,path,true),handle=await dir.getFileHandle(path.split('/').pop(),{create:true});
  const stream=await handle.createWritable();
  try{await stream.write(blob);await stream.close();}catch(e){try{await stream.abort();}catch(_){}throw e;}
  return handle;
}
async function restore(root,manifest,readFile,progress=()=>{},backupName) {
  validateManifest(manifest);
  backupName=backupName||new Date().toISOString().replace(/[:.]/g,'-')+'-'+crypto.randomUUID().slice(0,8);
  if(!/^[a-zA-Z0-9-]+$/.test(backupName))throw new Error('备份目录名称不正确');
  // Fetch and validate every remote file before altering any local record.
  const downloads=new Map();
  for(const file of manifest.files) {
    progress(`正在下载并校验 ${file.path}`);
    const blob=await readFile(file.path);
    if(blob.size!==file.size||await hash(blob)!==file.sha256)throw new Error(`文件校验失败：${file.path}，本地记录未覆盖`);
    downloads.set(file.path,blob);
  }
  const backupRoot=await root.getDirectoryHandle('netease_sync_backups',{create:true});
  // Do not reuse an existing backup directory, even for programmatic callers.
  try{await backupRoot.getDirectoryHandle(backupName);throw new Error('备份目录已存在，请重试');}
  catch(e){if(e.name!=='NotFoundError')throw e;}
  const backup=await backupRoot.getDirectoryHandle(backupName,{create:true}),originals=new Map();
  for(const file of manifest.files) {
    progress(`正在备份本地 ${file.path}`);
    const handle=await existing(root,file.path);
    if(handle){const blob=await handle.getFile(),sha=await hash(blob);await write(backup,file.path,blob);const saved=await existing(backup,file.path);if(await hash(await saved.getFile())!==sha)throw new Error('本地备份校验失败，停止覆盖');originals.set(file.path,sha);}
    else originals.set(file.path,null);
  }
  await write(backup,'RESTORE_MANIFEST.json',new Blob([JSON.stringify({export:manifest,originals:Object.fromEntries(originals)},null,2)],{type:'application/json'}));
  async function unchanged(path) {const handle=await existing(root,path),sha=handle?await hash(await handle.getFile()):null;if(sha!==originals.get(path))throw new Error(`本地 ${path} 在回传期间发生变化，请关闭本地扫描器后重试`);}
  for(const file of manifest.files)await unchanged(file.path);
  const attempted=[];
  try {
    for(const file of manifest.files) {
      await unchanged(file.path);
      progress(`正在覆盖 ${file.path}`);attempted.push(file.path);
      const handle=await write(root,file.path,downloads.get(file.path));
      if(await hash(await handle.getFile())!==file.sha256)throw new Error(`写入校验失败：${file.path}`);
    }
  } catch(error) {
    const failed=[];
    for(const path of attempted.reverse()) {
      try {
        if(originals.get(path)!==null){const handle=await existing(backup,path);const restored=await write(root,path,await handle.getFile());if(await hash(await restored.getFile())!==originals.get(path))throw new Error('恢复校验失败');}
        else {const dir=await directoryFor(root,path);await dir.removeEntry(path.split('/').pop());}
      } catch(_) {failed.push(path);}
    }
    throw new Error(`${error.message}\n${failed.length?'部分文件恢复失败：'+failed.join('、'):'已恢复本次尝试覆盖的原记录'}。备份位于 netease_sync_backups/${backupName}`);
  }
  return {count:manifest.files.length,backupPath:`netease_sync_backups/${backupName}`};
}
const api={validateManifest,hash,restore};
if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.RecordSync=api;
})(typeof window!=='undefined'?window:globalThis);
