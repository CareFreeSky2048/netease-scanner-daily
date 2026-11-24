// Exercise browser filesystem behavior using disposable in-memory directory handles.
const {test}=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
globalThis.crypto=require('node:crypto').webcrypto;
const sync=require('../static/records.js');
test('embedded scanner page scripts parse before deployment',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../templates/index.html'),'utf8');
  const scripts=[...html.matchAll(/<script([^>]*)>([\s\S]*?)<\/script>/gi)]
    .filter(([,attributes,body])=>!attributes.includes('src=')&&body.trim());
  assert.ok(scripts.length,'expected inline page script');
  for(const [, ,body] of scripts)new vm.Script(body,{filename:'templates/index.html'});
});
function missing(){return new DOMException('missing','NotFoundError');}
class Directory {
  constructor(name='Music',fault=null){this.name=name;this.files=new Map();this.dirs=new Map();this.fault=fault;}
  async getDirectoryHandle(name,{create=false}={}){if(!this.dirs.has(name)){if(!create)throw missing();this.dirs.set(name,new Directory(name));}return this.dirs.get(name);}
  async getFileHandle(name,{create=false}={}) {
    if(!this.files.has(name)){if(!create)throw missing();this.files.set(name,new Blob());}
    const dir=this;
    return {async getFile(){return dir.files.get(name);},async createWritable(){let data;return {async write(value){if(dir.fault?.(name))throw new Error('simulated disk full');data=value instanceof Blob?value:new Blob([value]);},async close(){dir.files.set(name,data);},async abort(){}};}};
  }
  async removeEntry(name){this.files.delete(name);}
}
async function manifest(records) {return {schema:1,created_at:'test',files:await Promise.all(Object.entries(records).map(async([path,blob])=>({path,size:blob.size,sha256:await sync.hash(blob)})))};}
test('overwrites exact files with backups; credentials and other caches survive',async()=>{
  const root=new Directory();root.files.set('VA_Tracks_Output.csv',new Blob(['old csv\r\n']));root.files.set('cookie.txt',new Blob(['local-cookie']));
  const caches=await root.getDirectoryHandle('VA_Artist_Caches',{create:true});caches.files.set('artists_ffffffffffffffff.json',new Blob(['old extra cache']));
  const records={'VA_Tracks_Output.csv':new Blob([new Uint8Array([239,187,191,65,13,10])]),'VA_Artist_Caches/artists_1234567890abcdef.json':new Blob(['{}'])};
  const result=await sync.restore(root,await manifest(records),p=>records[p],()=>{},'test-backup');
  assert.equal(result.count,2);assert.equal(await sync.hash(root.files.get('VA_Tracks_Output.csv')),await sync.hash(records['VA_Tracks_Output.csv']));
  assert.equal(await root.files.get('cookie.txt').text(),'local-cookie');assert.equal(await caches.files.get('artists_ffffffffffffffff.json').text(),'old extra cache');
  const backup=root.dirs.get('netease_sync_backups').dirs.get('test-backup');assert.equal(await backup.files.get('VA_Tracks_Output.csv').text(),'old csv\r\n');
});
test('corrupt remote bytes abort before backups or overwrite',async()=>{
  const root=new Directory();root.files.set('VA_Tracks_Output.csv',new Blob(['old']));const records={'VA_Tracks_Output.csv':new Blob(['new'])};
  await assert.rejects(sync.restore(root,await manifest(records),()=>new Blob(['bad']),()=>{},'test-backup'),/校验失败/);
  assert.equal(await root.files.get('VA_Tracks_Output.csv').text(),'old');assert.equal(root.dirs.size,0);
});
test('disallowed and traversal paths cannot reach filesystem',async()=>{
  for(const path of ['cookie.txt','../VA_Tracks_Output.csv','VA_Artist_Caches/cookie.json','VA_State.json'])assert.throws(()=>sync.validateManifest({schema:1,files:[{path,size:0,sha256:'0'.repeat(64)}]}),/不允许/);
});
test('write failure restores overwritten originals and removes new files',async()=>{
  let failed=false;const root=new Directory('Music',name=>{if(name==='VA_Scan_Metrics.json'&&!failed){failed=true;return true;}return false;});
  root.files.set('VA_Tracks_Output.csv',new Blob(['old']));
  const records={'VA_Tracks_Output.csv':new Blob(['new']),'VA_Album_Date_Cache.json':new Blob(['{}']),'VA_Scan_Metrics.json':new Blob(['{}'])};
  await assert.rejects(sync.restore(root,await manifest(records),p=>records[p],()=>{},'test-backup'),/已恢复/);
  assert.equal(await root.files.get('VA_Tracks_Output.csv').text(),'old');assert.equal(root.files.has('VA_Album_Date_Cache.json'),false);assert.equal(root.files.has('VA_Scan_Metrics.json'),false);
});
test('local writes during backup abort before records are overwritten',async()=>{
  const root=new Directory();root.files.set('VA_Tracks_Output.csv',new Blob(['old']));
  const records={'VA_Tracks_Output.csv':new Blob(['new']),'VA_Scan_Metrics.json':new Blob(['{}'])};
  await assert.rejects(sync.restore(root,await manifest(records),p=>records[p],message=>{if(message==='正在备份本地 VA_Scan_Metrics.json')root.files.set('VA_Tracks_Output.csv',new Blob(['changed externally']));},'test-backup'),/发生变化/);
  assert.equal(await root.files.get('VA_Tracks_Output.csv').text(),'changed externally');
});
