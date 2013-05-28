
import os
import errno
import __builtin__

import mox as mox_lib
from mox import IsA
from mox import IgnoreArg
from mox import stubout

from cinder import context
from cinder import exception
from cinder import test
from cinder import utils
from cinder.exception import ProcessExecutionError

from cinder.volume import nfs_vzhelezniakov


volume_test_info = {'id': '1234567890abcdef',
                    'name': 'test_volume',
                    }

NFS_MOUNT_POINT_BASE = '/opt/stack/cinder/mnt'


class NfsVZhelezniakovDriverTestCase(test.TestCase):
    """Test case for NFS vzhelezniakov driver"""

    def setUp(self):
        self._driver = nfs_vzhelezniakov.NFSDriver()
        self._mox = mox_lib.Mox()
        self.stubs = stubout.StubOutForTesting()

    def tearDown(self):
        self._mox.UnsetStubs()
        self.stubs.UnsetAll()

    def test_mount_point_permissions(self):
        mox = self._mox
        drv = self._driver

        mox.StubOutWithMock(os.path, 'exists')
        os.path.exists(NFS_MOUNT_POINT_BASE).AndReturn(True)

        mox.StubOutWithMock(os, 'access')
        os.access(NFS_MOUNT_POINT_BASE, os.R_OK | os.W_OK | os.EX_OK).AndReturn(False)

        mox.ReplayAll()

        self.assertRaises(exception.CinderException,
                          drv.do_setup, IsA(context.RequestContext))

        mox.VerifyAll()

    def test_local_path(self):
        mox = self._mox
        drv = self._driver
        path_output = NFS_MOUNT_POINT_BASE+'/'+volume_test_info['id']

        mox.StubOutWithMock(drv, '_execute')
        drv._execute('find',
                     NFS_MOUNT_POINT_BASE,
                     '-name',
                     volume_test_info['id'],
                     run_as_root=True).AndReturn((path_output, ''))

        mox.ReplayAll()

        self.assertEquals(NFS_MOUNT_POINT_BASE+'/'+volume_test_info['id'],
                          drv.local_path(volume_test_info))

        mox.VerifyAll()

    def test_initialize_connection(self):
        mox = self._mox
        drv = self._driver

        properties = dict()
        properties['volume_id'] = volume_test_info['id']
        properties['export'] = NFS_MOUNT_POINT_BASE+'/'+volume_test_info['id']
        properties['name'] = volume_test_info['name']

        conn_inf = {'driver_volume_type': 'nfs',
                    'data': properties
        }

        mox.StubOutWithMock(drv, 'local_path')
        drv.local_path(volume_test_info).\
            AndReturn(NFS_MOUNT_POINT_BASE+'/'+volume_test_info['id'])

        mox.ReplayAll()

        self.assertEqual(conn_inf,
                         drv.initialize_connection(volume_test_info, None))

        mox.VerifyAll()

    def test_ensure_export_raise_exception(self):
        mox = self._mox
        drv = self._driver

        mox.StubOutWithMock(drv, 'local_path')
        drv.local_path(volume_test_info).AndReturn('')

        mox.ReplayAll()

        self.assertRaises(exception.CinderException,
                          drv.ensure_export, None, volume_test_info)

        mox.VerifyAll()


    def test_delete_volume(self):
        mox = self._mox
        drv = self._driver
        path = NFS_MOUNT_POINT_BASE+'/'+volume_test_info['id']

        mox.StubOutWithMock(drv, 'local_path')
        drv.local_path(volume_test_info).AndReturn(path)

        mox.StubOutWithMock(drv, '_execute')
        drv._execute('rm',
                     '-f',
                     path,
                     run_as_root=True)

        mox.ReplayAll()

        drv.delete_volume(volume_test_info)

        mox.VerifyAll()

    def test_share_capacity(self):
        mox = self._mox
        drv = self._driver
        path = NFS_MOUNT_POINT_BASE+'/'+volume_test_info['id']
        df_out = ('Filesystem  1K-blocks  Used Available Use% Mounted on\n \
                  /dev/mapper/tutorialvm-root  14356000 4166152   9469568  31% /mnt/share1\n',
                  '')

        mox.StubOutWithMock(drv, '_execute')
        drv._execute('df',
                     path,
                     run_as_root=True).AndReturn(df_out)

        mox.ReplayAll()

        res_info = drv._share_capacity(path)
        self.assertEqual(9469568, res_info['capacity'])
        self.assertEqual('/mnt/share1', res_info['mounted_on'])

        mox.VerifyAll()

    def test_select_share_for_volume(self):
        mox = self._mox
        drv = self._driver

        mygen_1 = (line for line in ['172.18.194.34:/home/vzhelezniakov/Work/Mirantis/nfs_share',
                                   '172.18.194.34:/home/vzhelezniakov/Work/Mirantis/nfs_share2', ])

        mygen_2 = (line for line in [{'capacity': 9469568, 'mounted_on': '/mnt/share1', },
                                   {'capacity': 9469568, 'mounted_on': '/mnt/share1', },])


        mox.StubOutWithMock(os.path, 'exists')
        os.path.exists('/etc/cinder/shares.conf').AndReturn(True)

        mox.StubOutWithMock(utils, 'file_open')
        utils.file_open('/etc/cinder/shares.conf').AndReturn(mygen_1)

        mox.StubOutWithMock(drv, '_share_capacity')
        drv._share_capacity = lambda *args, **kwargs: mygen_2.next()

        mox.ReplayAll()

        drv._select_share_for_volume(1000000)

        mox.VerifyAll()