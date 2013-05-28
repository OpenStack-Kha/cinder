import os
import pwd
import grp
import time
from cinder import exception
from cinder import flags
from cinder.openstack.common import cfg
from cinder.openstack.common import log as logging
from cinder import utils
#from cinder.virt.xenapi import connection as xenapi_conn
#from cinder.virt.xenapi import volumeops
import cinder.volume.driver

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt('nfs_shares_config_vaz',
               default='/etc/cinder/shares.conf',
               help='Where the file with shares is located'),
    cfg.IntOpt('nfs_mount_point_base_vaz',
               default='$state_path/mnt',
               help='Base dir where nfs expected to be mounted'),
    cfg.IntOpt('nfs_disk_util_vaz',
               default='df',
               help='Use du or df for free space calculation'),
    cfg.IntOpt('nfs_sparsed_volumes_vaz',
               default=True,
               help='Create volumes as sparsed files which take no space.'
                    ' If set to False, volume is created as regular file.'
                    ' In such case volume creation takes a lot of time. '),
    cfg.StrOpt('nfs_mount_options_vaz',
               default=None,
               help='Mount options passed to the nfs client. See section '
                    'of the nfs man page for details.'),
]


FLAGS = flags.FLAGS
FLAGS.register_opts(volume_opts)


class NFSDriver(cinder.volume.driver.VolumeDriver):
    """NFS vzhelezniakov volume driver."""

    def __init__(self, *args, **kwargs):
        super(NFSDriver, self).__init__(*args, **kwargs)

    def do_setup(self, context):
        """
        Mount NFS shares
        """
        if not os.path.exists(FLAGS.nfs_mount_point_base_vaz):
            os.mkdir(FLAGS.nfs_mount_point_base, os.R_OK | os.W_OK | os.EX_OK)

        #check permissions
        if os.access(FLAGS.nfs_mount_point_base_vaz, os.R_OK | os.W_OK | os.EX_OK) is False:
            raise exception.CinderException('Incorrect permission to mount base folder')

        #mount shares
        uid = pwd.getpwnam("nobody").pw_uid
        gid = grp.getgrnam("nogroup").gr_gid

        if os.path.exists(FLAGS.nfs_shares_config_vaz):
            for nfs_share in utils.file_open(FLAGS.nfs_shares_config_vaz):
                external_path_name = nfs_share.strip()
                local_path_name = '%s/%s' % (FLAGS.nfs_mount_point_base_vaz, os.path.basename(external_path_name))

                # #try unmount
                # try:
                #     if os.path.exists(local_path_name):
                #      self._execute('umount.nfs',
                #                 local_path_name,
                #                 '-f',
                #                 run_as_root=True)
                # except Exception as ex:
                #     LOG.warn(ex.message)

                #try mount
                if not os.path.exists(local_path_name):
                    os.makedirs(local_path_name)
                    self._execute('chown',
                               'nobody:nogroup',
                               local_path_name,
                               run_as_root=True)
                    self._execute('mount.nfs',
                               external_path_name,
                               local_path_name,
                               run_as_root=True)

    def local_path(self, volume):
        """Return local path to existing local volume."""
        path = self._execute('find',
                             FLAGS.nfs_mount_point_base_vaz,
                             '-name',
                             volume['id'],
                             run_as_root=True)
        return path[0].strip()


    def _select_share_for_volume(self, size):
        """
        the best share, the share with max capacity
        :param size:
        :return:
        """
        best_share = {'capacity': 0, 'mounted_on': ''}

        if os.path.exists(FLAGS.nfs_shares_config_vaz):
            for nfs_share in utils.file_open(FLAGS.nfs_shares_config_vaz):
                external_path_name = nfs_share.strip()
                share_path = '%s/%s' % (FLAGS.nfs_mount_point_base_vaz,
                                             os.path.basename(external_path_name))

                volume_info = self._share_capacity(share_path)

                if volume_info['capacity'] > size and \
                   best_share['capacity'] < volume_info['capacity']:

                    best_share['capacity'] = volume_info['capacity']
                    best_share['mounted_on'] = volume_info['mounted_on']

        if best_share['capacity'] < size:
            raise exception.CinderException('No free space for new volume')

        return best_share

    def _share_capacity(self, path):
        """Return folder capacity"""
        volume_info = self._execute('df',
                                    path,
                                    run_as_root=True)

        _, volume_info, _ = volume_info[0].split('\n')
        volume_info = volume_info.split()

        return {'capacity': int(volume_info[3]),
                'mounted_on': volume_info[5]}

    def create_volume(self, volume):
        """Create volume."""
        path_name = self._select_share_for_volume(volume['size'])['mounted_on']

        file_name = path_name+'/'+volume['id']

        if FLAGS.nfs_sparsed_volumes:
            self._execute('truncate',
                          '-s',
                          self._sizestr(volume['size']),
                          file_name,
                          run_as_root=True)
        else:
            block_count = (1 << 30) * volume['size']
            self._execute('dd',
                          'if=/dev/zero',
                          'of=%s' % file_name,
                          'bs=1'
                          'count= %d' % block_count,
                           run_as_root=True)

        self._execute('chmod',
                      '0777',
                      file_name,
                      run_as_root=True)

    def delete_volume(self, volume):
        """
        Delete volume.
        Return ok if doesn't exist. Auto detach from all servers.
        """
        volume_path = self.local_path(volume)
        if volume_path:
            self._execute('rm',
                          '-f',
                          volume_path,
                          run_as_root=True)

    def initialize_connection(self, volume, connector):
        """Initializes the connection and returns connection info.
        """
        properties = {}
        properties['volume_id'] = volume['id']
        properties['export'] = self.local_path(volume)
        properties['name'] = volume['name']

        return {
            'driver_volume_type': 'nfs',
            'data': properties
        }

    def ensure_export(self, context, volume):
        path = self.local_path(volume)
        if not path:
            raise exception.CinderException('Volume doesn\'t exist')

    def create_export(self, context, volume):
        #print '~~~~~~~~~~~~~~~~~~NFSDriver. create_export'
        pass

    def remove_export(self, context, volume):
        #print '~~~~~~~~~~~~~~~~~~NFSDriver. remove_export'
        pass

    def check_for_export(self, context, volume_id):
        #print '~~~~~~~~~~~~~~~~~~NFSDriver. check_for_export'
        pass

    def terminate_connection(self, volume, connector):
        """ Detach volume from the initiator."""
        #print '~~~~~~~~~~~~~~~~~~NFSDriver. terminate_connection'
        pass

    def create_volume_from_snapshot(self, volume, snapshot):
        #print '~~~~~~~~~~~~~~~~~~NFSDriver. create_volume_from_snapshot'
        pass

    def create_snapshot(self, snapshot):
        #print '~~~~~~~~~~~~~~~~~~NFSDriver. create_snapshot'
        pass

    def delete_snapshot(self, snapshot):
        #print '~~~~~~~~~~~~~~~~~~NFSDriver. delete_snapshot'
        pass
