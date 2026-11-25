import { cp, mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const projectDir = resolve(scriptDir, '..')
const stageDir = await mkdtemp(join(tmpdir(), 'offsetdock-package-'))
const npmCli = process.env.npm_execpath

if (!npmCli) {
  throw new Error('npm_execpath is unavailable; run this packager through an npm script')
}

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, stdio: 'inherit', env: process.env })
  if (result.error) throw result.error
  if (result.status !== 0) {
    throw new Error(`${command} exited with status ${result.status ?? 'unknown'}`)
  }
}

function runNpm(args, cwd) {
  run(process.execPath, [npmCli, ...args], cwd)
}

try {
  runNpm(['run', 'build'], projectDir)

  await Promise.all([
    cp(join(projectDir, 'out'), join(stageDir, 'out'), { recursive: true }),
    cp(join(projectDir, 'resources'), join(stageDir, 'resources'), { recursive: true }),
    cp(join(projectDir, 'package.json'), join(stageDir, 'package.json')),
    cp(join(projectDir, 'package-lock.json'), join(stageDir, 'package-lock.json')),
    cp(join(projectDir, 'electron-builder.yml'), join(stageDir, 'electron-builder.yml'))
  ])

  runNpm(['ci', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund'], stageDir)

  const electronPackage = JSON.parse(
    await readFile(join(projectDir, 'node_modules', 'electron', 'package.json'), 'utf8')
  )
  const builderCli = join(projectDir, 'node_modules', 'electron-builder', 'cli.js')
  const builderArgs = [
    builderCli,
    ...process.argv.slice(2),
    `--config.electronVersion=${electronPackage.version}`,
    `--config.directories.output=${join(projectDir, 'dist')}`
  ]

  run(process.execPath, builderArgs, stageDir)
} finally {
  await rm(stageDir, { recursive: true, force: true })
}
