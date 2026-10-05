# Restore a prefix from a directory

Use this when the central tree has the wrong bytes and you have a directory that is the tree you want. That directory is the prefix. Extract an archive yourself before you start. `arborsync restore` does not open tar files.

`--prefix /` is the whole central tree. A path outside the prefix stays on disk and in the index.

After the first restore, every slave has to run this version. A slave binary that does not know `RestoreEpochs` logs a bad frame and disconnects. It does not put the deleted files back.

## Stop the daemons

Stop the master. If a slave is older than this version, stop it too. Slaves on this version can stay up. They converge after the master starts again.

## Extract the archive

If the good tree is in an archive, extract it to a directory. The next command reads that directory.

## Run restore

```
arborsync restore --config /etc/arborsync/master.toml --source /tmp/good-src --prefix /src
```

`--source` is the directory that becomes `--prefix`. A file `source/foo` is written to `{prefix}/foo`. Omit `--prefix` to restore `/`.

The command prints the prefix, the generation, and how many paths it copied, replaced, and deleted. A second run of the same tree prints that the prefix already matches.

If the master is still running, the command fails and tells you to stop the master.

If the command stops while files are still copying, run it again before you start the master. The retry keeps the generation it already chose. The master does not start while that prefix is still replacing.

## See what a slave would pull

```
arborsync restore --config /etc/arborsync/master.toml --source /tmp/good-src --prefix /src --pretend
```

`--pretend` writes nothing. It does not need the master to be stopped.

The lines are the paths a slave would take from central if its checkout were `--source` and `last_synced` matched those files. `replace /src/foo` is a file or symlink whose `FileNode` differs. `delete /src/old` is a path only on central. A file that exists only in `--source` is not listed. That slave would upload it.

No lines means that slave would only push and would pull nothing. You do not need `restore` for this prefix.

## Start the master

Start the master with the same config as before.

## What you should see

Slaves drop extra files under the prefix, replace changed bytes, and pull missing files. A slave binary that does not know `RestoreEpochs` logs a bad frame and disconnects. Upgrade it and start it again.
