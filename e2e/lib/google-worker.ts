import fs from 'node:fs';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {expect} from '@playwright/test';
import {API_BASE,BINS,REPO,STATE_DIR,backendEnv} from '../stack/config.mjs';

// Reboot only this checkout's recorded serving process. Real startup sweeps run
// against existing state; no seeded captures or modified production cadence.
export async function restartGoogleWorker() {
  const file=path.join(STATE_DIR,'state.json');
  const state=JSON.parse(fs.readFileSync(file,'utf8'));
  if(state.api!==API_BASE || !state.pids.backend) throw new Error('stack ownership mismatch');
  process.kill(state.pids.backend,'SIGTERM');
  await expect.poll(async()=>{try{return !(await fetch(`${API_BASE}/readyz`)).ok;}catch{return true;}},{timeout:30_000}).toBe(true);
  const log=fs.openSync(path.join(STATE_DIR,'backend.log'),'a');
  const child=spawn(BINS.backend,[],{cwd:REPO,env:{...process.env,...backendEnv()},stdio:['ignore',log,log],detached:true});
  child.unref(); fs.closeSync(log);
  state.pids.backend=child.pid;
  fs.writeFileSync(file,JSON.stringify(state,null,2));
  await expect.poll(async()=>{try{return (await fetch(`${API_BASE}/readyz`)).ok;}catch{return false;}},{timeout:30_000}).toBe(true);
}
