#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import parted

# Basic support for command line history. fdisk(1) doesn't do this,
# so we are even better!
import readline

import sys


class ExitMainLoop(Exception):
    """Exception that signals the job is done"""
    pass


class UnknownCommand(Exception):
    """Exception raised when user enters invalid command"""
    pass


class Fdisk(object):
    """Main program class"""

    def __init__(self, devpath):
        # Supported commands and corresponding handlers
        self.commands = {
            'a': self.toggle_bootable,
            'd': self.delete_partition,
            'm': self.print_menu,
            'n': self.add_partition,
            'o': self.create_empty,
            'p': self.print_partitions,
            'q': self.quit,
            'w': self.write
        }

        try:
            self.device = parted.getDevice(devpath)
        except parted.IOException as e:
            raise RuntimeError(e.message)

        try:
            self.disk = parted.newDisk(self.device)
            if self.disk.type != 'msdos':
                raise RuntimeError('Only MBR partitions are supported')
        except parted.DiskException:
            self.create_empty()

    def do_command(self, cmd):
        if cmd in self.commands:
            return self.commands[cmd]()
        else:
            raise UnknownCommand()

    def toggle_bootable(self):
        """toggle a bootable flag"""
        if not self.disk.partitions:
            print 'No partition is defined yet!'
            return

        number = self._ask_partition()
        for p in self.disk.partitions:
            # handle partitions not in disk order
            if p.number == number:
                if not p.getFlag(parted.PARTITION_BOOT):
                    p.setFlag(parted.PARTITION_BOOT)
                else:
                    p.unsetFlag(parted.PARTITION_BOOT)
                break

    def delete_partition(self):
        """delete a partition"""
        if not self.disk.partitions:
            print 'No partition is defined yet!'

        number = self._ask_partition()
        for p in self.disk.partitions:
            # handle partitions not in disk order
            if p.number == number:
                try:
                    self.disk.deletePartition(p)
                except parted.PartitionException as e:
                    print e.message
                break

    def print_menu(self):
        """print this menu"""
        print "Command action"
        for (command, fun) in self.commands.iteritems():
            print '{0:^7}{1}'.format(command, fun.__doc__)

    def add_partition(self):
        """add a new partition"""
        # Primary partitions count
        pri_count = len(self.disk.getPrimaryPartitions())
        # HDDs may contain only one extended partition
        ext_count = 1 if self.disk.getExtendedPartition() else 0
        # First logical partition number
        lpart_start = self.disk.maxPrimaryPartitionCount + 1
        # Number of spare partitions slots
        parts_avail = self.disk.maxPrimaryPartitionCount - (pri_count + ext_count)

        data = {
            'primary': pri_count,
            'extended': ext_count,
            'free': parts_avail,
            'first_logical': lpart_start
        }
        default = None
        options = set()

        geometry = self._get_largest_free_region()
        if not geometry:
            print 'No free sectors available'
            return

        if not parts_avail and not ext_count:
            print """If you want to create more than four partitions, you must replace a
primary partition with an extended partition first."""
            return
        else:
            print "Partition type:"
            if parts_avail:
                default = 'p'
                options.add('p')
                print '   p   primary ({primary:d} primary, {extended:d} extended, {free:d} free)'.format(**data)
                if not ext_count:
                    # If we have only one spare partition, suggest extended
                    if pri_count >= 3:
                        default = 'e'
                    options.add('e')
                    print '   e   extended'
            if ext_count:
                # XXX: We do not observe disk.getMaxLogicalPartitions() constraint
                default = default or 'l'
                options.add('l')
                print '   l   logical (numbered from {first_logical:d})'.format(**data)

        # fdisk doesn't display a menu if it has only one option, but we do
        choice = raw_input('Select (default {default:s}): '.format(default=default))
        if not choice:
            print "Using default response {default:s}".format(default=default)
            choice = default

        if not choice[0] in options:
            print "Invalid partition type `{choice}'".format(choice=choice)
            return

        try:
            partition = None
            ext_part = self.disk.getExtendedPartition()
            if choice[0] == 'p':
                # If there is an extended partition, we look for free region that is
                # completely outside of it.
                if ext_part:
                    try:
                        ext_part.geometry.intersect(geometry)
                        print 'No free sectors available'
                        return
                    except ArithmeticError:
                        # All ok
                        pass

                partition = self._create_partition(geometry, type=parted.PARTITION_NORMAL)
            elif choice[0] == 'e':
                # Create extended partition in the largest free region
                partition = self._create_partition(geometry, type=parted.PARTITION_EXTENDED)
            elif choice[0] == 'l':
                # Largest free region must be (at least partially) inside the
                # extended partition.
                try:
                    geometry = ext_part.geometry.intersect(geometry)
                except ArithmeticError:
                    print "No free sectors available"
                    return

                partition = self._create_partition(geometry, type=parted.PARTITION_LOGICAL)

            if partition:
                print 'Partition number {number:d} created'.format(number=partition.number)
        except RuntimeError as e:
            print e.message

    def create_empty(self):
        """create a new empty DOS partition table"""
        print """
Device contains no valid DOS partition table.
Building a new DOS disklabel.
Changes will remain in memory only, until you decide to write them.
After that, of course, the previous content won't be recoverable."""
        self.disk = parted.freshDisk(self.device, 'msdos')

    def print_partitions(self):
        """print the partition table"""
        # Shortcuts
        device, disk = self.device, self.disk
        unit = device.sectorSize
        size = device.length * device.sectorSize
        cylinders, heads, sectors = device.hardwareGeometry
        data = {
            'path': device.path,
            'size': size,
            'size_mbytes': int(parted.formatBytes(size, 'MB')),
            'heads': heads,
            'sectors': sectors,
            'cylinders': cylinders,
            'sectors_total': device.length,
            'unit': unit,
            'sector_size': device.sectorSize,
            'physical_sector_size': device.physicalSectorSize,
            # Try to guess minimum_io_size and optimal_io_size, should work under Linux
            'minimum_io_size': device.minimumAlignment.grainSize * device.sectorSize,
            'optimal_io_size': device.optimumAlignment.grainSize * device.sectorSize,
        }

        # TODO: Alignment offset: disk.startAlignment if disk.startAlignment != 0
        print """
Disk {path}: {size_mbytes:d} MB, {size:d} bytes
{heads:d} heads, {sectors:d} sectors/track, {cylinders:d} cylinders, total {sectors_total:d} sectors
Units = 1 * sectors of {unit:d} = {unit:d} bytes
Sector size (logical/physical): {sector_size:d} bytes / {physical_sector_size:d} bytes
I/O size (minimum/optimal): {minimum_io_size:d} bytes / {optimal_io_size:d} bytes
""".format(**data)

        # Calculate first column width: if there is something in it, there should be enough space
        # If not, give it the minimum to fit the caption nicely
        width = len(disk.partitions[0].path) if disk.partitions else len('Device') + 1
        print "{0:>{width}} Boot      Start         End      Blocks   Id  System".format('Device', width=width)

        for p in disk.partitions:
            boot = '*' if p.getFlag(parted.PARTITION_BOOT) else ''
            # Assume default 1K-blocks
            blocks = int(parted.formatBytes(p.geometry.length * device.sectorSize, 'KiB'))
            data = {
                'path': p.path,
                'boot': boot,
                'start': p.geometry.start,
                'end': p.geometry.end,
                'blocks': blocks,
                'id': p.number,
                'system': self._guess_system(p),
            }
            print "{path:>}{boot:>4}{start:>12d}{end:>12d}{blocks:>12d}{id:>5d}  {system}".format(**data)

        return device, disk

    def quit(self):
        """quit without saving change"""
        raise ExitMainLoop()

    def write(self):
        """write table to disk and exit"""
        self.disk.commit()
        raise ExitMainLoop()

    # Protected helper methods

    def _ask_value(self, prompt, default=None, parse=None):
        """Asks user for a value using prompt, parse using parse if necessary"""
        value = None
        while not value:
            choice = raw_input(prompt)
            if not choice:
                if default:
                    print 'Using default value {default:s}'.format(default=str(default))
                    value = default
            else:
                try:
                    value = parse(choice) if parse else choice
                except ValueError:
                    print "Invalid value"
                    continue
        return value

    def _ask_partition(self):
        """Asks user for a partition number"""
        last_part = self.disk.lastPartitionNumber
        number = self._ask_value("Partition number (1-{last_part:d}): ".format(last_part=last_part),
                                 parse=lambda x: int(x))
        return number

    def _parse_last_sector_expr(self, start, value, sector_size):
        """Parses fdisk(1)-style partition end exception"""
        import re

        # map fdisk units to PyParted ones
        known_units = {'K': 'KiB', 'M': 'MiB', 'G': 'GiB',
                       'KB': 'kB', 'MB': 'MB', 'GB': 'GB'}

        match = re.search('^\+(?P<num>\d+)(?P<unit>[KMG]?)$', value)
        if match:
            # num must be an integer; if not, raise ValueError
            num = int(match.group('num'))
            unit = match.group('unit')
            if not unit:
                # +sectors
                sector = start + num
                return sector
            elif unit in known_units.keys():
                # +size{K,M,G}
                sector = start + parted.sizeToSectors(num, known_units[unit], sector_size)
                return sector
        else:
            # must be an integer (sector); if not, raise ValueError
            sector = int(value)
            return sector

    def _create_partition(self, region, type=parted.PARTITION_NORMAL):
        """Creates the partition with geometry specified"""
        # libparted doesn't let setting partition number, so we skip it, too

        # We want our new partition to be optimally aligned
        # (you can use minimalAlignedConstraint, if you wish).
        alignment = self.device.optimalAlignedConstraint
        constraint = parted.Constraint(maxGeom=region).intersect(alignment)
        data = {
            'start': constraint.startAlign.alignUp(region, region.start),
            'end': constraint.endAlign.alignDown(region, region.end),
        }

        # Ideally, ask_value() should immediately check that value is in range. We sacrifice
        # this feature to demonstrate the exception raised when a partition doesn't meet the constraint.
        part_start = self._ask_value(
            'First sector ({start:d}-{end:d}, default {start:d}): '.format(**data),
            data['start'],
            lambda x: int(x))
        part_end = self._ask_value(
            'Last sector, +sectors or +size{{K,M,G}} ({start:d}-{end:d}, default {end:d}): '.format(**data),
            data['end'],
            lambda x: self._parse_last_sector_expr(part_start, x, self.device.sectorSize))

        try:
            partition = parted.Partition(
                disk=self.disk,
                type=type,
                geometry=parted.Geometry(device=self.device, start=part_start, end=part_end)
            )
            self.disk.addPartition(partition=partition, constraint=constraint)
        except (parted.PartitionException, parted.GeometryException, parted.CreateException) as e:
            # GeometryException accounts for incorrect start/end values (e.g. start < end),
            # CreateException is raised e.g. when the partition doesn't fit on the disk.
            # PartedException is a generic error (e.g. start/end values out of range)
            raise RuntimeError(e.message)

        return partition

    def _guess_system(self, partition):
        """Tries to guess partition type"""
        if not partition.fileSystem:
            if partition.getFlag(parted.PARTITION_SWAP):
                return 'Linux swap / Solaris'
            elif partition.getFlag(parted.PARTITION_RAID):
                return 'Linux raid autodetect'
            elif partition.getFlag(parted.PARTITION_LVM):
                return 'Linux LVM'
            else:
                return 'unknown'
        else:
            if partition.fileSystem.type in {'ext2', 'ext3', 'ext4', 'btrfs', 'reiserfs', 'xfs', 'jfs'}:
                return 'Linux'
            # If mkswap(1) was run on the partition, upper branch won't be executed
            elif partition.fileSystem.type.startswith('linux-swap'):
                return 'Linux swap / Solaris'
            elif partition.fileSystem.type == 'fat32':
                return 'W95 FAT32'
            elif partition.fileSystem.type == 'fat16':
                return 'W95 FAT16'
            elif partition.fileSystem.type == 'ntfs':
                return 'HPFS/NTFS/exFAT'
            else:
                return 'unknown'

    def _get_largest_free_region(self):
        """Finds largest free region on the disk"""
        # There are better ways to do it, but let's be straightforward
        max_size = -1
        region = None

        alignment = self.device.optimumAlignment

        for r in self.disk.getFreeSpaceRegions():
            # Heuristic: Ignore alignment gaps
            if r.length > max_size and r.length > alignment.grainSize:
                region = r
                max_size = r.length

        return region


def usage():
    print """
Usage:
 fdisk <disk> change partition table
"""


def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    try:
        fdisk = Fdisk(devpath=sys.argv[1])
    except RuntimeError as e:
        print e.message
        sys.exit(1)

    while True:
        c = raw_input('\nCommand (m for help): ')
        try:
            fdisk.do_command(c[0])
        except UnknownCommand:
            fdisk.print_menu()
        except ExitMainLoop:
            sys.exit(0)


if __name__ == '__main__':
    main()
