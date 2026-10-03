"""Offline Chrome channel launch with a fresh profile; no LinkedIn access."""
import json
import os
from pathlib import Path
import subprocess
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT.parent / 'NOS-V1/M3/node_modules/playwright'
PROFILE = ROOT / '.local' / ('offline-browser-' + uuid4().hex)


def main():
    script = '''
const { chromium } = require(process.argv[1]);
(async () => {
  let context;
  try {
    context = await chromium.launchPersistentContext(process.argv[2], {channel:'chrome',headless:true,timeout:10000});
    await context.route('**/*', route => route.abort('blockedbyclient'));
    const page = context.pages()[0] || await context.newPage();
    await page.goto('about:blank');
    console.log(JSON.stringify({launched:true,only_navigation:'about:blank'}));
  } catch (error) {
    console.log(JSON.stringify({launched:false,error_kind:error.name,
      chrome_install_not_found:/executable.*doesn.t exist|distribution.*not found/i.test(error.message)}));
  } finally { if (context) await context.close(); }
})();
'''
    keys = {'PATH','PATHEXT','SYSTEMROOT','WINDIR','TEMP','TMP','LOCALAPPDATA','USERPROFILE','APPDATA','COMSPEC','SYSTEMDRIVE'}
    outcomes = []
    for mode in ('old_filtered', 'with_install_paths'):
        if mode == 'with_install_paths': keys |= {'PROGRAMFILES', 'PROGRAMFILES(X86)', 'PROGRAMW6432', 'HOMEDRIVE', 'HOMEPATH'}
        env = {key: value for key, value in os.environ.items() if key.upper() in keys}
        profile = PROFILE / mode
        result = subprocess.run(['node','-e',script,str(SOURCE),str(profile)], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=20,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        outcomes.append({'mode':mode,'exit_code':result.returncode,'result':json.loads(result.stdout)})
    report = {'source_navigations':0,'profile':str(PROFILE.relative_to(ROOT)),'outcomes':outcomes}
    (ROOT / 'docs/results/browser-environment-verification.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2))


if __name__ == '__main__': main()
