import os
class Scanner:
    def __init__(self, search_path='/'):
        self.search_path = search_path
        # compute excluded devices/mounts (boot/root and mdadm members)
        self._excluded_mounts = self._compute_excluded_mounts()
        # record mountpoints that this scanner has mounted during a pass
        self._last_mounted = []
        # base dir used for manual mounts
        self._auto_mount_root = '/mnt/auto_media'

    def _read_mounts(self):
        mounts = []
        with open('/proc/mounts', 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    dev, mnt = parts[0], parts[1]
                    mounts.append((dev, mnt))
        return mounts

    def _device_basename(self, devnode):
        # normalize device node like "/dev/sda1" -> "sda", "/dev/nvme0n1p1" -> "nvme0n1"
        if not devnode or not devnode.startswith('/dev/'):
            return None
        name = devnode.rsplit('/', 1)[-1]
        sys_path = f'/sys/class/block/{name}'
        try:
            if os.path.exists(sys_path):
                real = os.path.realpath(sys_path)
                parent = os.path.basename(os.path.dirname(real))
                if parent and parent != name:
                    return parent
        except Exception:
            pass
        # fallback heuristic: strip known partition suffix patterns
        # nvme/mmcblk use 'p' before number, normal disks just digits
        m = __import__('re').match(r'^(?P<base>.*?)(p?\d+)?$', name)
        if m:
            return m.group('base')
        return name

    def _parse_mdstat_members(self):
        members = set()
        try:
            with open('/proc/mdstat', 'r', encoding='utf-8') as f:
                for line in f:
                    # lines that contain member devices usually have tokens like "sda1[0]"
                    tokens = line.strip().split()
                    for t in tokens:
                        # extract token that looks like a block device (letters+digits, e.g. sda1, nvme0n1p1)
                        m = __import__('re').match(r'^([a-zA-Z0-9_-]+?\d+)', t)
                        if m:
                            members.add('/dev/' + m.group(1))
        except FileNotFoundError:
            pass
        return members

    def _compute_excluded_mounts(self):
        import os
        import logging
        mounts = self._read_mounts()
        # gather devices mounted at / and /boot (if present)
        important_mounts = {'/','/boot'}
        important_devs = set()
        for dev, mnt in mounts:
            if mnt in important_mounts and dev.startswith('/dev/'):
                important_devs.add(dev)
        # mdadm member devices
        md_members = self._parse_mdstat_members()
        # include base devices for important_devs and md members
        excluded_devnames = set()
        for d in list(important_devs) + list(md_members):
            bn = self._device_basename(d)
            if bn:
                excluded_devnames.add(bn)
            # also add the exact dev path
            if d.startswith('/dev/'):
                excluded_devnames.add(d.rsplit('/', 1)[-1])
        # find mountpoints whose device basename is in excluded_devnames
        excluded_mounts = set()
        for dev, mnt in mounts:
            if dev.startswith('/dev/'):
                devname = dev.rsplit('/', 1)[-1]
                if devname in excluded_devnames:
                    excluded_mounts.add(mnt)
        # also, if search_path itself is on an excluded device, exclude it
        try:
            st = os.stat(self.search_path)
            # find mountpoint that contains this path
            candidate = None
            for dev, mnt in mounts:
                if self.search_path == mnt or self.search_path.startswith(mnt.rstrip('/') + '/'):
                    candidate = (dev, mnt)
            if candidate:
                devname = candidate[0].rsplit('/', 1)[-1]
                if devname in excluded_devnames:
                    excluded_mounts.add(candidate[1])
        except Exception:
            pass
        result = sorted(excluded_mounts, key=len, reverse=True)
        logger = logging.getLogger(__name__)
        logger.info("Excluded device basenames: %s", sorted(excluded_devnames))
        logger.info("Excluded mountpoints: %s", result if result else "<none>")
        return result

    def _is_excluded_path(self, path):
        """
        Return True if path is equal to an excluded mountpoint or is a subpath
        of one. Special-case '/' so it does not match everything.
        """
        for ex in self._excluded_mounts:
            if path == ex:
                return True
            # avoid treating '/' as a prefix for all absolute paths
            trimmed = ex.rstrip('/')
            if trimmed and path.startswith(trimmed + '/'):
                return True
        return False

    def _is_removable_device(self, devnode):
        # check /sys/class/block/<base>/removable (returns '1' for removable)
        import os
        base = self._device_basename(devnode)
        if not base:
            return False
        path = f'/sys/class/block/{base}/removable'
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read().strip() == '1'
        except Exception:
            return False

    def _candidate_mounts(self):
        """
        Return a list of mountpoints that are likely to be "plugged in" drives:
        - mounts whose backing block device is marked removable in sysfs
        - mounts under common user/media paths: /media, /run/media, /mnt
        Excludes mounts that are part of the excluded set (boot/root/mdadm).
        """
        mounts = self._read_mounts()
        candidates = []
        for dev, mnt in mounts:
            if not dev.startswith('/dev/'):
                continue
            # skip explicitly excluded mountpoints
            if self._is_excluded_path(mnt):
                continue
            devname = dev.rsplit('/', 1)[-1]
            # skip loop devices, ram, boot mount and optical device nodes
            if devname.startswith('loop') or devname.startswith('ram') or mnt.startswith('/boot'):
                continue
            # include removable devices, common media/mnt locations, and also
            # non-removable block-device mounts (e.g. SATA SSDs) unless they're
            # explicitly excluded or are loop/ram/sr device types.
            if (self._is_removable_device(dev)
                    or mnt.startswith('/media') or mnt.startswith('/run/media') or mnt.startswith('/mnt')
                    or (dev.startswith('/dev/') and not devname.startswith(('loop', 'ram', 'sr')))):
                candidates.append(mnt)

        # THIS WAS CAUSING ISSUES WITH NFS/SMB MOUNTS; COMMENTING OUT FOR NOW
        # also include subdirectories of /run.media/<user> and /media if they exist but weren't mounted with a device name 
        # for base in ('/run/media', '/media', '/mnt'):
        #     try:
        #         if os.path.exists(base):
        #             for entry in os.listdir(base):
        #                 p = os.path.join(base, entry)
        #                 if os.path.ismount(p) and p not in candidates and not any(p == ex or (ex.rstrip('/') and p.startswith(ex.rstrip('/') + '/')) for ex in self._excluded_mounts):
        #                     candidates.append(p)
        #     except Exception:
        #         pass

        # unique, stable ordering and log candidates
        result = sorted(set(candidates), key=len, reverse=True)
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Candidate mountpoints: %s", result if result else "<none>")
        return result

    def _mount_device(self, devnode):
        """
        Try to mount devnode. Prefer udisksctl (desktop-friendly); fall back to plain mount.
        Returns mountpoint on success, None on failure.
        """
        import subprocess, os, logging, shlex
        logger = logging.getLogger(__name__)
        devpath = devnode if devnode.startswith('/dev/') else f'/dev/{devnode}'

        def probe_fstype(dev):
            try:
                b = subprocess.run(['blkid', '-o', 'value', '-s', 'TYPE', dev],
                                   capture_output=True, text=True, check=False)
                f = b.stdout.strip()
                if f:
                    return f
            except Exception:
                pass
            try:
                l = subprocess.run(['lsblk', '-no', 'FSTYPE', dev],
                                   capture_output=True, text=True, check=False)
                f = l.stdout.strip()
                if f:
                    return f
            except Exception:
                pass
            return ''

        fstype = probe_fstype(devpath)
        logger.debug("Probed fstype for %s -> %s", devpath, fstype or "<unknown>")

        # try udisksctl first (capture and log output)
        try:
            res = subprocess.run(['udisksctl', 'mount', '-b', devpath],
                                 capture_output=True, text=True, check=False)
            logger.debug("udisksctl rc=%s stdout=%s stderr=%s", res.returncode, res.stdout.strip(), res.stderr.strip())
            if res.returncode == 0:
                out = res.stdout + res.stderr
                for part in out.splitlines():
                    if ' at ' in part:
                        toks = part.split(' at ', 1)[1].rstrip('.').strip()
                        if os.path.ismount(toks):
                            return toks
                try:
                    fm = subprocess.run(['findmnt', '-n', '-o', 'TARGET', devpath],
                                        capture_output=True, text=True, check=False)
                    tgt = fm.stdout.strip()
                    if tgt:
                        return tgt
                except Exception:
                    pass
            else:
                logger.debug("udisksctl failed to mount %s", devpath)
        except FileNotFoundError:
            logger.debug("udisksctl not found")
        except Exception as e:
            logger.debug("udisksctl mount attempt raised: %s", e)

        # fallback: try manual mounts (explicit exfat helpers tried before generic mount)
        mroot = self._auto_mount_root
        try:
            os.makedirs(mroot, exist_ok=True)
            target = os.path.join(mroot, os.path.basename(devpath))
            os.makedirs(target, exist_ok=True)

            mount_cmds = []
            # try explicit detected fstype first
            if fstype:
                mount_cmds.append(['mount', '-t', fstype, devpath, target])

            # try exfat-specific helpers and explicit -t exfat BEFORE generic mount
            mount_cmds.append(['mount', '-t', 'exfat', devpath, target])
            mount_cmds.append(['mount.exfat', devpath, target])
            mount_cmds.append(['mount.exfat-fuse', devpath, target])

            # generic mount last
            mount_cmds.append(['mount', devpath, target])

            last_err = None
            for cmd in mount_cmds:
                try:
                    logger.debug("Attempting mount: %s", shlex.join(cmd))
                    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
                    logger.debug("mount cmd=%s rc=%s stdout=%s stderr=%s", shlex.join(cmd), res.returncode, res.stdout.strip(), res.stderr.strip())
                    if res.returncode == 0 and os.path.ismount(target):
                        return target
                    last_err = (cmd, res.returncode, res.stdout.strip(), res.stderr.strip())
                except FileNotFoundError as e:
                    logger.debug("mount helper not found for %s: %s", shlex.join(cmd), e)
                    last_err = (cmd, 'missing', '', str(e))
                except Exception as e:
                    logger.debug("mount raised for %s: %s", shlex.join(cmd), e)
                    last_err = (cmd, 'exception', '', str(e))

            # cleanup and log last error
            try:
                os.rmdir(target)
            except Exception:
                pass
            if last_err:
                cmd, rc, out, err = last_err
                logger.debug("All mount attempts failed for %s. last=%s rc=%s out=%s err=%s",
                             devpath, shlex.join(cmd), rc, out, err)
            return None
        except Exception as e:
            logger.debug("Failed to prepare mountpoint for %s: %s", devpath, e)
            return None

    def ensure_mounted_candidates(self):
        """
        Return a list of mountpoints that should be scanned. This will:
        - include already-mounted candidate mountpoints
        - attempt to mount unmounted partition devices (non-excluded) and include their mountpoints
        """
        import subprocess, os, logging
        mounts = self._read_mounts()
        mounted_points = {mnt for dev, mnt in mounts if dev.startswith('/dev/')}
        # reset last-mounted list for this pass
        self._last_mounted = []
        scan_roots = list(self._candidate_mounts())  # already-mounted and common mounts

        # find unmounted partitions via lsblk and attempt to mount them
        try:
            # include FSTYPE column to help with mount decisions (and better parsing)
            ls = subprocess.run(['lsblk', '-nr', '-o', 'NAME,TYPE,MOUNTPOINT'],
                                capture_output=True, text=True, check=True)
            for line in ls.stdout.splitlines():
                parts = line.split(None, 3)
                if len(parts) < 2:
                    continue
                name, typ = parts[0], parts[1]
                mp = parts[2].strip() if len(parts) >= 3 else ''
                # fstype = parts[3].strip() if len(parts) >= 4 else ''
                devnode = f'/dev/{name}'
                # skip non-partition types or already-mounted entries
                if typ != 'part' or mp:
                    continue
                # skip excluded devices if we can map them (we only have excluded mountpoints list,
                # so skip names that match excluded mount basenames)
                if any(name == os.path.basename(ex) or f'/dev/{name}' == ex for ex in self._excluded_mounts):
                    continue
                if name.startswith(('loop', 'ram', 'sr')):
                    continue
                # attempt mount
                tgt = self._mount_device(devnode)
                if tgt:
                    logging.info("Mounted %s -> %s", devnode, tgt)
                    # if this mount wasn't present before, record it so we can unmount later
                    if tgt not in mounted_points:
                        self._last_mounted.append(tgt)
                    scan_roots.append(tgt)
        except Exception:
            # be conservative: do not raise, just return what we have
            pass

        # dedupe and exclude explicitly excluded mountpoints
        return scan_roots

    def find_video_files(self, scan_roots=None):
        import os, logging
        logger = logging.getLogger(__name__)
        video_extensions = ('.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv')
        video_extensions_lower = tuple(e.lower() for e in video_extensions)
        video_files = []
        excluded_mounts = self._excluded_mounts

        # determine roots
        if scan_roots is not None:
            search_paths = list(scan_roots)
        elif self.search_path and self.search_path != '/':
            search_paths = [self.search_path]
        else:
            try:
                search_paths = self.ensure_mounted_candidates()
            except Exception:
                search_paths = self._candidate_mounts()

        logger.info("Scanner will walk these roots: %s", ", ".join(search_paths) if search_paths else "<none>")

        for search_root in search_paths:
            # first check for optical disc structures and prefer them over generic walk
            try:
                dvd_files = self._scan_dvd_mount(search_root)
                if dvd_files:
                    video_files.append(dvd_files)
                    logger.info("Collected %d DVD files from %s", len(dvd_files), search_root)
                    continue
                bluray_files = self._scan_bluray_mount(search_root)
                if bluray_files:
                    video_files.append(bluray_files)
                    logger.info("Collected %d Blu-ray files from %s", len(bluray_files), search_root)
                    continue
            except Exception:
                logger.debug("Optical scan check failed for %s", search_root)
            if not os.path.exists(search_root):
                logger.debug("Search root does not exist: %s", search_root)
                continue
            if not os.path.ismount(search_root):
                logger.debug("Search root is not a mountpoint (may still be valid): %s", search_root)
            found_in_root = 0
            for root, dirs, files in os.walk(search_root, topdown=True):
                # skip excluded subtrees entirely (do not process files here or descend)
                if self._is_excluded_path(root):
                    continue
                # Prune only directories that are explicitly excluded, otherwise descend into all subdirectories
                good_dirs = []
                for d in dirs:
                    full = os.path.join(root, d)
                    if not self._is_excluded_path(full):
                        good_dirs.append(d)
                dirs[:] = good_dirs

                for file in files:
                    try:
                        if file.lower().endswith(video_extensions_lower):
                            full = os.path.join(root, file)
                            video_files.append(full)
                            found_in_root += 1
                            logger.debug("Matched file: %s", full)
                    except Exception as e:
                        logger.debug("Error checking file %s/%s: %s", root, file, e)
            logger.info("Found %d candidate video files under %s", found_in_root, search_root)

        logger.info("Total candidate video files found: %d", len(video_files))
        return video_files

    def unmount_mountpoints(self, mountpoints=None):
        """
        Unmount the provided mountpoints (or any recorded last-mounted points if None).
        Uses udisksctl unmount -b <source> when possible, otherwise falls back to 'umount <mountpoint>'.
        Removes empty mount directories under the scanner's auto-mount root.
        Returns a list of successfully-unmounted paths.
        """
        import subprocess, logging, os
        logger = logging.getLogger(__name__)
        to_unmount = list(mountpoints) if mountpoints else list(self._last_mounted)
        succeeded = []
        for mp in to_unmount:
            try:
                # find the backing source (device) for the mount
                src = None
                try:
                    fm = subprocess.run(['findmnt', '-n', '-o', 'SOURCE', '--target', mp],
                                        capture_output=True, text=True, check=False)
                    src = fm.stdout.strip() if fm.returncode == 0 else None
                except Exception:
                    src = None

                unmounted = False
                # try udisksctl unmount if we have a device path
                if src and src.startswith('/dev/'):
                    try:
                        r = subprocess.run(['udisksctl', 'unmount', '-b', src],
                                           capture_output=True, text=True, check=False)
                        logger.debug("udisksctl unmount rc=%s out=%s err=%s", r.returncode, r.stdout.strip(), r.stderr.strip())
                        if r.returncode == 0:
                            unmounted = True
                    except FileNotFoundError:
                        logger.debug("udisksctl not found for unmount")
                    except Exception as e:
                        logger.debug("udisksctl unmount raised: %s", e)

                # fallback to plain umount by mountpoint
                if not unmounted:
                    try:
                        r = subprocess.run(['umount', mp], capture_output=True, text=True, check=False)
                        logger.debug("umount rc=%s out=%s err=%s", r.returncode, r.stdout.strip(), r.stderr.strip())
                        if r.returncode == 0:
                            unmounted = True
                    except Exception as e:
                        logger.debug("umount failed for %s: %s", mp, e)

                if unmounted:
                    logger.info("Unmounted %s", mp)
                    succeeded.append(mp)
                    # remove auto-created mount dir if under auto-mount root and empty
                    try:
                        if mp.startswith(self._auto_mount_root) and os.path.isdir(mp) and not os.listdir(mp):
                            os.rmdir(mp)
                            logger.debug("Removed empty mount directory %s", mp)
                    except Exception:
                        logger.debug("Failed to remove mount dir %s", mp)
                else:
                    logger.debug("Failed to unmount %s", mp)
            except Exception as e:
                logger.debug("Error unmounting %s: %s", mp, e)

        # clear recorded mounts that succeeded
        self._last_mounted = [m for m in self._last_mounted if m not in succeeded]
        return succeeded

    def _scan_dvd_mount(self, mount):
        """Detect DVD structure (VIDEO_TS) and return list of VOB/IFO/BUP files (largest-first)."""
        import logging
        logger = logging.getLogger(__name__)
        try:
            video_ts = os.path.join(mount, "VIDEO_TS")
            if not os.path.isdir(video_ts):
                return []
            entries = []
            for name in os.listdir(video_ts):
                up = name.upper()
                if up.endswith(".VOB") or up.endswith(".IFO") or up.endswith(".BUP"):
                    return video_ts
                    path = os.path.join(video_ts, name)
                    try:
                        size = os.path.getsize(path)
                    except Exception:
                        size = 0
                    
                    entries.append((size, video_ts))
            if not entries:
                return []
            # prefer VOBs ordered by size, then other files
            entries.sort(reverse=True)
            files = [p for _, p in entries]
            logger.info("Detected DVD at %s, returning %d files (largest-first)", mount, len(files))
            return files
        except Exception as e:
            logging.getLogger(__name__).debug("DVD scan failed for %s: %s", mount, e)
            return []

    def _scan_bluray_mount(self, mount):
        """Detect Blu-ray structure (BDMV/STREAM) and return list of .m2ts files (largest-first)."""
        import logging
        logger = logging.getLogger(__name__)
        try:
            stream_dir = os.path.join(mount, "BDMV", "STREAM")
            if not os.path.isdir(stream_dir):
                return []
            entries = []
            for name in os.listdir(stream_dir):
                if name.lower().endswith(".m2ts"):
                    return stream_dir
            if not entries:
                return []
            entries.sort(reverse=True)
            files = [p for _, p in entries]
            logger.info("Detected Blu-ray at %s, returning %d .m2ts files (largest-first)", mount, len(files))
            return files
        except Exception as e:
            logging.getLogger(__name__).debug("Blu-ray scan failed for %s: %s", mount, e)
            return []