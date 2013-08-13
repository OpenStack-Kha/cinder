# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2012 NetApp, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""
Volume driver for NetApp NFS storage.
"""

import copy
import os
import re
import socket
import time

from cinder import exception
from cinder.image import image_utils
from cinder.openstack.common import log as logging
from cinder import units
from cinder import utils
from cinder.volume.drivers.netapp.api import NaApiError
from cinder.volume.drivers.netapp.api import NaElement
from cinder.volume.drivers.netapp.api import NaServer
from cinder.volume.drivers.netapp.options import netapp_basicauth_opts
from cinder.volume.drivers.netapp.options import netapp_connection_opts
from cinder.volume.drivers.netapp.options import netapp_img_cache_opts
from cinder.volume.drivers.netapp.options import netapp_transport_opts
from cinder.volume.drivers.netapp.utils import provide_ems
from cinder.volume.drivers.netapp.utils import validate_instantiation
from cinder.volume.drivers import nfs
from oslo.config import cfg
from threading import Timer


LOG = logging.getLogger(__name__)


CONF = cfg.CONF
CONF.register_opts(netapp_connection_opts)
CONF.register_opts(netapp_transport_opts)
CONF.register_opts(netapp_basicauth_opts)
CONF.register_opts(netapp_img_cache_opts)


class NetAppNFSDriver(nfs.NfsDriver):
    """Base class for NetApp NFS driver.
      Executes commands relating to Volumes.
    """
    def __init__(self, *args, **kwargs):
        # NOTE(vish): db is set by Manager
        validate_instantiation(**kwargs)
        self._execute = None
        self._context = None
        super(NetAppNFSDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(netapp_connection_opts)
        self.configuration.append_config_values(netapp_basicauth_opts)
        self.configuration.append_config_values(netapp_transport_opts)
        self.configuration.append_config_values(netapp_img_cache_opts)

    def set_execute(self, execute):
        self._execute = execute

    def do_setup(self, context):
        super(NetAppNFSDriver, self).do_setup(context)

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met."""
        raise NotImplementedError()

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        vol_size = volume.size
        snap_size = snapshot.volume_size

        if vol_size != snap_size:
            msg = _('Cannot create volume of size %(vol_size)s from '
                    'snapshot of size %(snap_size)s')
            msg_fmt = {'vol_size': vol_size, 'snap_size': snap_size}
            raise exception.CinderException(msg % msg_fmt)

        self._clone_volume(snapshot.name, volume.name, snapshot.volume_id)
        share = self._get_volume_location(snapshot.volume_id)

        return {'provider_location': share}

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        self._clone_volume(snapshot['volume_name'],
                           snapshot['name'],
                           snapshot['volume_id'])

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        nfs_mount = self._get_provider_location(snapshot.volume_id)

        if self._volume_not_present(nfs_mount, snapshot.name):
            return True

        self._execute('rm', self._get_volume_path(nfs_mount, snapshot.name),
                      run_as_root=True)

    def _get_client(self):
        """Creates client for server."""
        raise NotImplementedError()

    def _get_volume_location(self, volume_id):
        """Returns NFS mount address as <nfs_ip_address>:<nfs_mount_dir>."""
        nfs_server_ip = self._get_host_ip(volume_id)
        export_path = self._get_export_path(volume_id)
        return (nfs_server_ip + ':' + export_path)

    def _clone_volume(self, volume_name, clone_name, volume_id):
        """Clones mounted volume with OnCommand proxy API."""
        raise NotImplementedError()

    def _get_provider_location(self, volume_id):
        """Returns provider location for given volume."""
        volume = self.db.volume_get(self._context, volume_id)
        return volume.provider_location

    def _get_host_ip(self, volume_id):
        """Returns IP address for the given volume."""
        return self._get_provider_location(volume_id).split(':')[0]

    def _get_export_path(self, volume_id):
        """Returns NFS export path for the given volume."""
        return self._get_provider_location(volume_id).split(':')[1]

    def _volume_not_present(self, nfs_mount, volume_name):
        """Check if volume exists."""
        try:
            self._try_execute('ls', self._get_volume_path(nfs_mount,
                                                          volume_name))
        except exception.ProcessExecutionError:
            # If the volume isn't present
            return True
        return False

    def _try_execute(self, *command, **kwargs):
        # NOTE(vish): Volume commands can partially fail due to timing, but
        #             running them a second time on failure will usually
        #             recover nicely.
        tries = 0
        while True:
            try:
                self._execute(*command, **kwargs)
                return True
            except exception.ProcessExecutionError:
                tries = tries + 1
                if tries >= self.configuration.num_shell_tries:
                    raise
                LOG.exception(_("Recovering from a failed execute.  "
                                "Try number %s"), tries)
                time.sleep(tries ** 2)

    def _get_volume_path(self, nfs_share, volume_name):
        """Get volume path (local fs path) for given volume name on given nfs
        share.

        @param nfs_share string, example 172.18.194.100:/var/nfs
        @param volume_name string,
            example volume-91ee65ec-c473-4391-8c09-162b00c68a8c
        """
        return os.path.join(self._get_mount_point_for_share(nfs_share),
                            volume_name)

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        vol_size = volume.size
        src_vol_size = src_vref.size

        if vol_size != src_vol_size:
            msg = _('Cannot create clone of size %(vol_size)s from '
                    'volume of size %(src_vol_size)s')
            msg_fmt = {'vol_size': vol_size, 'src_vol_size': src_vol_size}
            raise exception.CinderException(msg % msg_fmt)

        self._clone_volume(src_vref.name, volume.name, src_vref.id)
        share = self._get_volume_location(src_vref.id)

        return {'provider_location': share}

    def _update_volume_status(self):
        """Retrieve status info from volume group."""
        super(NetAppNFSDriver, self)._update_volume_status()
        self._spawn_clean_cache_job()

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""
        super(NetAppNFSDriver, self).copy_image_to_volume(
                context, volume, image_service, image_id)
        LOG.info(_('Copied image to volume %s'), volume['name'])
        self._register_image_in_cache(volume, image_id)

    def _register_image_in_cache(self, volume, image_id, share=None):
        """Stores image in the cache."""
        file_name = 'img-cache-%s' % image_id
        LOG.info(_("Registering image in cache %s"), file_name)
        if not share:
            share = self._get_provider_location(volume['id'])
        try:
            self._do_clone_rel_img_cache(
                volume['name'], file_name, share, file_name)
        except Exception as e:
            LOG.warn(
                _('Exception while registering image %(image_id)s'
                ' in cache. Exception: %(exc)s')
                % {'image_id': image_id, 'exc': e.__str__()})

    def _find_image_in_cache(self, image_id):
        """Finds image in cache and returns list of shares with file name."""
        result = []
        if getattr(self, '_mounted_shares', None):
            for share in self._mounted_shares:
                dir = self._get_mount_point_for_share(share)
                file_name = 'img-cache-%s' % image_id
                file_path = '%s/%s' % (dir, file_name)
                if os.path.exists(file_path):
                    LOG.debug(_('Found cache file for image %(image_id)s'
                        ' on share %(share)s')
                        % {'image_id': image_id, 'share': share})
                    result.append((share, file_name))
        return result

    def _do_clone_rel_img_cache(self, src, dst, share, cache_file):
        """Do clone operation w.r.t image cache file."""
        @utils.synchronized(cache_file, external=True)
        def _do_clone():
            dir = self._get_mount_point_for_share(share)
            file_path = '%s/%s' % (dir, dst)
            if not os.path.exists(file_path):
                LOG.info(_('Cloning img from cache for %s'), dst)
                self._clone_volume(src, dst, volume_id=None, share=share)
        _do_clone()

    @utils.synchronized('clean_cache')
    def _spawn_clean_cache_job(self):
        """Spawns a clean task if not running."""
        if getattr(self, 'cleaning', None):
                LOG.debug(_('Image cache cleaning in progress. Returning... '))
                return
        else:
                #set cleaning to True
                self.cleaning = True
                t = Timer(0, self._clean_image_cache)
                t.start()

    def _clean_image_cache(self):
        """Clean the image cache files in cache of space crunch."""
        try:
            LOG.debug(_('Image cache cleaning in progress.'))
            thres_size_perc_start =\
                self.configuration.thres_avl_size_perc_start
            thres_size_perc_stop =\
                self.configuration.thres_avl_size_perc_stop
            for share in getattr(self, '_mounted_shares', []):
                try:
                    total_size, total_avl, total_alc =\
                        self._get_capacity_info(share)
                    avl_percent = int((total_avl / total_size) * 100)
                    if avl_percent <= thres_size_perc_start:
                        LOG.info(_('Cleaning cache for share %s.'), share)
                        eligible_files = self._find_old_cache_files(share)
                        threshold_size = int(
                            (thres_size_perc_stop * total_size) / 100)
                        bytes_to_free = int(threshold_size - total_avl)
                        LOG.debug(_('Files to be queued for deletion %s'),
                            eligible_files)
                        self._delete_files_till_bytes_free(
                            eligible_files, share, bytes_to_free)
                    else:
                        continue
                except Exception, ex:
                    LOG.warn(_(
                        'Exception during cache cleaning'
                        ' share %s. Message - %s') % (share, ex))
                    continue
        finally:
            LOG.debug(_('Image cache cleaning done.'))
            self.cleaning = False

    def _shortlist_del_eligible_files(self, share, old_files):
        """Prepares list of eligible files to be deleted from cache."""
        raise NotImplementedError()

    def _find_old_cache_files(self, share):
        """Finds the old files in cache."""
        mount_fs = self._get_mount_point_for_share(share)
        threshold_minutes = self.configuration.expiry_thres_minutes
        cmd = ['find', mount_fs, '-maxdepth', '1', '-name',
                     'img-cache*', '-amin', '+%s' % (threshold_minutes)]
        res, __ = self._execute(*cmd, run_as_root=True)
        if res:
            old_file_paths = res.strip('\n').split('\n')
            mount_fs_len = len(mount_fs)
            old_files = [x[mount_fs_len + 1:] for x in old_file_paths]
            eligible_files = self._shortlist_del_eligible_files(
                share, old_files)
            return eligible_files
        return []

    def _delete_files_till_bytes_free(self, file_list, share, bytes_to_free=0):
        """Delete files from disk till bytes are freed or list exhausted."""
        LOG.debug(_('Bytes to free %s'), bytes_to_free)
        if file_list and bytes_to_free > 0:
            sorted_files = sorted(file_list, key=lambda x: x[1], reverse=True)
            mount_fs = self._get_mount_point_for_share(share)
            for f in sorted_files:
                if f:
                    file_path = '%s/%s' % (mount_fs, f[0])
                    LOG.debug(_('Delete file path %s'), file_path)

                    @utils.synchronized(f[0], external=True)
                    def _do_delete():
                        if self._delete_file(file_path):
                            return True
                        return False
                    if _do_delete():
                            bytes_to_free = bytes_to_free - int(f[1])
                            if bytes_to_free <= 0:
                                return

    def _delete_file(self, path):
        """Delete file from disk and return result as boolean."""
        try:
            LOG.debug(_('Deleting file at path %s'), path)
            cmd = ['rm', '-f', path]
            self._execute(*cmd, run_as_root=True)
            return True
        except Exception, ex:
            LOG.warning(_('Exception during deleting %s'), ex)
            return False

    def clone_image(self, volume, image_location, image_id=None):
        """Create a volume efficiently from an existing image.

        image_location is a string whose format depends on the
        image service back end in use. The driver should use it
        to determine whether cloning is possible.

        Returns a boolean indicating whether cloning occurred
        """
        cloned = False
        share = None
        image_location = self._construct_image_nfs_direct_url(image_location)
        try:
            cache_result = self._find_image_in_cache(image_id)
            if cache_result:
                LOG.info(_('Found image in cache, id: %s'), image_id)
                for res in cache_result:
                    # Repeat tries in other shares if failed in some
                    (share, file_name) = res
                    LOG.debug(_('Cache  share: %s'), share)
                    if (share and
                        self._is_share_eligible(share, volume['size'])):
                        try:
                            dir_path = self._get_mount_point_for_share(share)
                            self._do_clone_rel_img_cache(
                                file_name, volume['name'], share, file_name)
                            cloned = self._post_img_clone(volume, dir_path)
                            break
                        except:
                            LOG.warn(_('Unexpected exception in'
                                ' img clone in share %s'), share)
            else:
                share = self._is_cloneable_share(image_location)
                if share and self._is_share_eligible(share, volume['size']):
                    LOG.debug(_('Share is cloneable %s'), share)
                    (__, ___, img_file) = image_location.rpartition('/')
                    dir_path = self._get_mount_point_for_share(share)
                    img_path = '%s/%s' % (dir_path, img_file)
                    img_info = image_utils.qemu_img_info(img_path)
                    if img_info.file_format == 'raw':
                        LOG.debug(_('Image is raw %s'), image_id)
                        self._clone_volume(
                            img_file, volume['name'],
                            volume_id=None, share=share)
                        cloned = True
                    else:
                        LOG.info(
                            _('Image will locally be converted to raw %s'),
                            image_id)
                        dst = '%s/%s' % (dir_path, volume['name'])
                        image_utils.convert_image(img_path, dst, 'raw')
                        data = image_utils.qemu_img_info(dst)
                        if data.file_format != "raw":
                            LOG.warn(_("Converted to raw, but"
                                " format is now %s"),
                                data.file_format)
                            os.unlink(dst)
                            cloned = False
                        else:
                            cloned = True
                            self._register_image_in_cache(
                                volume, image_id, share)
                if cloned:
                    cloned = self._post_img_clone(volume, dir_path)
        except Exception as e:
            LOG.warn(_('Unexpected exception in cloning image'
                ' %(image_id)s. Exception: %(exc)s')
                % {'image_id': image_id, 'exc': e.__str__()})
        finally:
            share = share if cloned else None
            return {'provider_location': share, 'bootable': True}, cloned

    def _post_img_clone(self, volume, dir_path):
        """Do operations post efficient image cloning."""
        LOG.info(_('Performing post clone for %s'), volume['name'])
        op_result = False
        try:
            vol_path = '%s/%s' % (dir_path, volume['name'])
            if self._discover_file_till_timeout(vol_path):
                self._set_rw_permissions_for_all(vol_path)
                op_result = self._resize_vol(
                    volume, dir_path, delete_on_fail=True)
        except Exception as e:
            LOG.warn(_('Unexpected exception in post image'
                ' clone for %(name)s. Exception:%(exc)s')
                % {'name': volume['name'], 'exc': e.__str__()})
        finally:
            return op_result

    def _resize_vol(self, volume, dir_path, delete_on_fail=True):
        """Resize the volume on share and delete on fail."""
        LOG.debug(_('Resizing volume %s'), volume['name'])
        vol_path = '%s/%s' % (dir_path, volume['name'])
        try:
            if self._is_file_size_equal(vol_path, volume['size']):
                return True
            else:
                image_utils.resize_image(vol_path, volume['size'])
                if self._is_file_size_equal(vol_path, volume['size']):
                    return True
                else:
                    raise exception.InvalidVolume(
                        reason=_('Resizing volume  failed %s.')
                        % volume['name'])
        except exception.InvalidVolume as e:
            LOG.warn(_('Operation falied. Msg-%s'), e.message)
            if delete_on_fail and os.path.exists(vol_path):
                os.unlink(vol_path)
            return False
        except Exception as e:
            LOG.warn(
                _('Unexpected exception during resizing'
                ' volume %(name)s. Exception: %(exc)s')
                % {'name': volume['name'], 'exc': e.__str__()})
            if delete_on_fail and os.path.exists(vol_path):
                os.unlink(vol_path)
            return False

    def _is_file_size_equal(self, path, size):
        """Checks if file size at path is equal to size."""
        data = image_utils.qemu_img_info(path)
        virt_size = data.virtual_size / units.GiB
        if virt_size == size:
            return True
        else:
            return False

    def _discover_file_till_timeout(self, path):
        """Checks if file size at path is equal to size."""
        # Sometimes nfs takes time to discover file
        # Retrying in case any unexpected situation occurs
        retry_seconds = 45
        sleep_interval = 2
        while True:
            if os.path.exists(path):
                return True
            else:
                if retry_seconds <= 0:
                    LOG.warn(_('Discover file retries exhausted.'))
                    return False
                else:
                    time.sleep(sleep_interval)
                    retry_seconds = retry_seconds - sleep_interval

    def _is_cloneable_share(self, image_location):
        """Finds if the image at location is cloneable.

             WebNFS url format with relative-path is supported.
             Accepting all characters in path-names and checking
             against the mounted shares which will contain only
             allowed path segments.
        """
        nfs_loc_pattern =\
           '^nfs://(([\w\-\.]+:{1}[\d]+|[\w\-\.]+)(/[^\/].*)*(/[^\/\\\\]+)$)'
        matched = re.match(nfs_loc_pattern, image_location, flags=0)
        if not matched:
            LOG.debug(_('Image location not in the'
                ' expected format %s'), image_location)
            return None
        conn = matched.group(2)
        dir = matched.group(3) or '/'
        return self._check_share_in_use(conn, dir)

    def _share_match_for_ip(self, ip, shares):
        """Returns the share that is served by ip."""
        raise NotImplementedError()

    def _check_share_in_use(self, conn, dir):
        """Checks if share is mounted and returns it. """
        try:
            if conn:
                host = conn.split(':')[0]
                ipv4 = socket.gethostbyname(host)
                share_candidates = []
                for sh in self._mounted_shares:
                    sh_exp = sh.split(':')[1]
                    if sh_exp == dir:
                        share_candidates.append(sh)
                if share_candidates:
                    LOG.debug(_('Found possible share matches %s'),
                        share_candidates)
                    return self._share_match_for_ip(ipv4, share_candidates)
        except:
            LOG.warn(_("Unexpected exception while short listing used share."))
        return None


class NetAppDirectNfsDriver (NetAppNFSDriver):
    """Executes commands related to volumes on NetApp filer."""

    def __init__(self, *args, **kwargs):
        super(NetAppDirectNfsDriver, self).__init__(*args, **kwargs)

    def do_setup(self, context):
        super(NetAppDirectNfsDriver, self).do_setup(context)
        self._context = context
        self.check_for_setup_error()
        self._client = self._get_client()
        self._do_custom_setup(self._client)

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met."""
        self._check_flags()

    def _clone_volume(self, volume_name, clone_name, volume_id):
        """Clones mounted volume on NetApp filer."""
        raise NotImplementedError()

    def _check_flags(self):
        """Raises error if any required configuration flag for NetApp
        filer is missing.
        """
        required_flags = ['netapp_login',
                          'netapp_password',
                          'netapp_server_hostname',
                          'netapp_server_port',
                          'netapp_transport_type']
        for flag in required_flags:
            if not getattr(self.configuration, flag, None):
                raise exception.CinderException(_('%s is not set') % flag)

    def _get_client(self):
        """Creates NetApp api client."""
        client = NaServer(
            host=self.configuration.netapp_server_hostname,
            server_type=NaServer.SERVER_TYPE_FILER,
            transport_type=self.configuration.netapp_transport_type,
            style=NaServer.STYLE_LOGIN_PASSWORD,
            username=self.configuration.netapp_login,
            password=self.configuration.netapp_password)
        return client

    def _do_custom_setup(self, client):
        """Do the customized set up on client if any for different types."""
        raise NotImplementedError()

    def _is_naelement(self, elem):
        """Checks if element is NetApp element."""
        if not isinstance(elem, NaElement):
            raise ValueError('Expects NaElement')

    def _get_ontapi_version(self):
        """Gets the supported ontapi version."""
        ontapi_version = NaElement('system-get-ontapi-version')
        res = self._client.invoke_successfully(ontapi_version, False)
        major = res.get_child_content('major-version')
        minor = res.get_child_content('minor-version')
        return (major, minor)

    def _get_export_ip_path(self, volume_id=None, share=None):
        """Returns export ip and path.

          One of volume id or share is used to return the values.
        """
        if volume_id:
            host_ip = self._get_host_ip(volume_id)
            export_path = self._get_export_path(volume_id)
        elif share:
            host_ip = share.split(':')[0]
            export_path = share.split(':')[1]
        else:
            raise exception.InvalidInput('None of vol id or share specified.')
        return (host_ip, export_path)

    def _create_file_usage_req(self, path):
        """Creates the request element for file_usage_get."""
        file_use = NaElement.create_node_with_children(
            'file-usage-get', **{'path': path})
        return file_use


class NetAppDirectCmodeNfsDriver (NetAppDirectNfsDriver):
    """Executes commands related to volumes on c mode."""

    def __init__(self, *args, **kwargs):
        super(NetAppDirectCmodeNfsDriver, self).__init__(*args, **kwargs)

    def _do_custom_setup(self, client):
        """Do the customized set up on client for cluster mode."""
        # Default values to run first api
        client.set_api_version(1, 15)
        (major, minor) = self._get_ontapi_version()
        client.set_api_version(major, minor)

    def _invoke_successfully(self, na_element, vserver=None):
        """Invoke the api for successful result.

        If vserver is present then invokes vserver api
        else Cluster api.
        :param vserver: vserver name.
        """
        self._is_naelement(na_element)
        server = copy.copy(self._client)
        if vserver:
            server.set_vserver(vserver)
        else:
            server.set_vserver(None)
        result = server.invoke_successfully(na_element, True)
        return result

    def _clone_volume(self, volume_name, clone_name,
                      volume_id=None, share=None):
        """Clones mounted volume on NetApp Cluster."""
        (vserver, exp_volume) = self._get_vserver_and_exp_vol(volume_id, share)
        self._clone_file(exp_volume, volume_name, clone_name, vserver)

    def _get_vserver_and_exp_vol(self, volume_id=None, share=None):
        """Gets the vserver and export volume for share."""
        (host_ip, export_path) = self._get_export_ip_path(volume_id, share)
        ifs = self._get_if_info_by_ip(host_ip)
        vserver = ifs[0].get_child_content('vserver')
        exp_volume = self._get_vol_by_junc_vserver(vserver, export_path)
        return (vserver, exp_volume)

    def _get_if_info_by_ip(self, ip):
        """Gets the network interface info by ip."""
        net_if_iter = NaElement('net-interface-get-iter')
        net_if_iter.add_new_child('max-records', '10')
        query = NaElement('query')
        net_if_iter.add_child_elem(query)
        query.add_node_with_children('net-interface-info', **{'address': ip})
        result = self._invoke_successfully(net_if_iter)
        if result.get_child_content('num-records') and\
                int(result.get_child_content('num-records')) >= 1:
            attr_list = result.get_child_by_name('attributes-list')
            return attr_list.get_children()
        raise exception.NotFound(
            _('No interface found on cluster for ip %s')
            % (ip))

    def _get_vol_by_junc_vserver(self, vserver, junction):
        """Gets the volume by junction path and vserver."""
        vol_iter = NaElement('volume-get-iter')
        vol_iter.add_new_child('max-records', '10')
        query = NaElement('query')
        vol_iter.add_child_elem(query)
        vol_attrs = NaElement('volume-attributes')
        query.add_child_elem(vol_attrs)
        vol_attrs.add_node_with_children(
            'volume-id-attributes',
            **{'junction-path': junction,
            'owning-vserver-name': vserver})
        des_attrs = NaElement('desired-attributes')
        des_attrs.add_node_with_children('volume-attributes',
                                         **{'volume-id-attributes': None})
        vol_iter.add_child_elem(des_attrs)
        result = self._invoke_successfully(vol_iter, vserver)
        if result.get_child_content('num-records') and\
                int(result.get_child_content('num-records')) >= 1:
            attr_list = result.get_child_by_name('attributes-list')
            vols = attr_list.get_children()
            vol_id = vols[0].get_child_by_name('volume-id-attributes')
            return vol_id.get_child_content('name')
        msg_fmt = {'vserver': vserver, 'junction': junction}
        raise exception.NotFound(_("""No volume on cluster with vserver
                                   %(vserver)s and junction path %(junction)s
                                   """) % msg_fmt)

    def _clone_file(self, volume, src_path, dest_path, vserver=None):
        """Clones file on vserver."""
        msg = _("""Cloning with params volume %(volume)s,src %(src_path)s,
                    dest %(dest_path)s, vserver %(vserver)s""")
        msg_fmt = {'volume': volume, 'src_path': src_path,
                   'dest_path': dest_path, 'vserver': vserver}
        LOG.debug(msg % msg_fmt)
        clone_create = NaElement.create_node_with_children(
            'clone-create',
            **{'volume': volume, 'source-path': src_path,
            'destination-path': dest_path})
        self._invoke_successfully(clone_create, vserver)

    def _update_volume_status(self):
        """Retrieve status info from volume group."""
        super(NetAppDirectCmodeNfsDriver, self)._update_volume_status()
        netapp_backend = 'NetApp_NFS_cluster_direct'
        backend_name = self.configuration.safe_get('volume_backend_name')
        self._stats["volume_backend_name"] = (backend_name or
                                              netapp_backend)
        self._stats["vendor_name"] = 'NetApp'
        self._stats["driver_version"] = '1.0'
        provide_ems(self, self._client, self._stats, netapp_backend)

    def _shortlist_del_eligible_files(self, share, old_files):
        """Prepares list of eligible files to be deleted from cache."""
        file_list = []
        (vserver, exp_volume) = self._get_vserver_and_exp_vol(
            volume_id=None, share=share)
        for file in old_files:
            path = '/vol/%s/%s' % (exp_volume, file)
            u_bytes = self._get_cluster_file_usage(path, vserver)
            file_list.append((file, u_bytes))
        LOG.debug(_('Shortlisted del elg files %s'), file_list)
        return file_list

    def _get_cluster_file_usage(self, path, vserver):
        """Gets the file unique bytes."""
        LOG.debug(_('Getting file usage for %s'), path)
        file_use = NaElement.create_node_with_children(
            'file-usage-get', **{'path': path})
        res = self._invoke_successfully(file_use, vserver)
        bytes = res.get_child_content('unique-bytes')
        LOG.debug(_('file-usage for path %(path)s is %(bytes)s')
             % {'path': path, 'bytes': bytes})
        return bytes

    def _share_match_for_ip(self, ip, shares):
        """Returns the share that is served by ip."""
        ip_vserver = self._get_vserver_for_ip(ip)
        if ip_vserver and shares:
            for share in shares:
                ip_sh = share.split(':')[0]
                sh_vserver = self._get_vserver_for_ip(ip_sh)
                if sh_vserver == ip_vserver:
                    LOG.debug(_('Share match found for ip %s'), ip)
                    return share
        LOG.debug(_('No share match found for ip %s'), ip)
        return None

    def _get_vserver_for_ip(self, ip):
        """Get vserver for the mentioned ip."""
        try:
            ifs = self._get_if_info_by_ip(ip)
            vserver = ifs[0].get_child_content('vserver')
            return vserver
        except:
            return None


class NetAppDirect7modeNfsDriver (NetAppDirectNfsDriver):
    """Executes commands related to volumes on 7 mode."""

    def __init__(self, *args, **kwargs):
        super(NetAppDirect7modeNfsDriver, self).__init__(*args, **kwargs)

    def _do_custom_setup(self, client):
        """Do the customized set up on client if any for 7 mode."""
        (major, minor) = self._get_ontapi_version()
        client.set_api_version(major, minor)

    def _invoke_successfully(self, na_element, vfiler=None):
        """Invoke the api for successful result.

        If vfiler is present then invokes vfiler api
        else filer api.
        :param vfiler: vfiler name.
        """
        self._is_naelement(na_element)
        server = copy.copy(self._client)
        if vfiler:
            server.set_vfiler(vfiler)
        else:
            server.set_vfiler(None)
        result = server.invoke_successfully(na_element, True)
        return result

    def _clone_volume(self, volume_name, clone_name,
                      volume_id=None, share=None):
        """Clones mounted volume with NetApp filer."""
        (host_ip, export_path) = self._get_export_ip_path(volume_id, share)
        storage_path = self._get_actual_path_for_export(export_path)
        target_path = '%s/%s' % (storage_path, clone_name)
        (clone_id, vol_uuid) = self._start_clone('%s/%s' % (storage_path,
                                                            volume_name),
                                                 target_path)
        if vol_uuid:
            try:
                self._wait_for_clone_finish(clone_id, vol_uuid)
            except NaApiError as e:
                if e.code != 'UnknownCloneId':
                    self._clear_clone(clone_id)
                raise e

    def _get_actual_path_for_export(self, export_path):
        """Gets the actual path on the filer for export path."""
        storage_path = NaElement.create_node_with_children(
            'nfs-exportfs-storage-path', **{'pathname': export_path})
        result = self._invoke_successfully(storage_path, None)
        if result.get_child_content('actual-pathname'):
            return result.get_child_content('actual-pathname')
        raise exception.NotFound(_('No storage path found for export path %s')
                                 % (export_path))

    def _start_clone(self, src_path, dest_path):
        """Starts the clone operation.

        :returns: clone-id
        """
        msg_fmt = {'src_path': src_path, 'dest_path': dest_path}
        LOG.debug(_("""Cloning with src %(src_path)s, dest %(dest_path)s""")
                  % msg_fmt)
        clone_start = NaElement.create_node_with_children(
            'clone-start',
            **{'source-path': src_path,
            'destination-path': dest_path,
            'no-snap': 'true'})
        result = self._invoke_successfully(clone_start, None)
        clone_id_el = result.get_child_by_name('clone-id')
        cl_id_info = clone_id_el.get_child_by_name('clone-id-info')
        vol_uuid = cl_id_info.get_child_content('volume-uuid')
        clone_id = cl_id_info.get_child_content('clone-op-id')
        return (clone_id, vol_uuid)

    def _wait_for_clone_finish(self, clone_op_id, vol_uuid):
        """Waits till a clone operation is complete or errored out."""
        clone_ls_st = NaElement('clone-list-status')
        clone_id = NaElement('clone-id')
        clone_ls_st.add_child_elem(clone_id)
        clone_id.add_node_with_children('clone-id-info',
                                        **{'clone-op-id': clone_op_id,
                                        'volume-uuid': vol_uuid})
        task_running = True
        while task_running:
            result = self._invoke_successfully(clone_ls_st, None)
            status = result.get_child_by_name('status')
            ops_info = status.get_children()
            if ops_info:
                state = ops_info[0].get_child_content('clone-state')
                if state == 'completed':
                    task_running = False
                elif state == 'failed':
                    code = ops_info[0].get_child_content('error')
                    reason = ops_info[0].get_child_content('reason')
                    raise NaApiError(code, reason)
                else:
                    time.sleep(1)
            else:
                raise NaApiError(
                    'UnknownCloneId',
                    'No clone operation for clone id %s found on the filer'
                    % (clone_id))

    def _clear_clone(self, clone_id):
        """Clear the clone information.

        Invoke this in case of failed clone.
        """
        clone_clear = NaElement.create_node_with_children(
            'clone-clear',
            **{'clone-id': clone_id})
        retry = 3
        while retry:
            try:
                self._invoke_successfully(clone_clear, None)
                break
            except Exception as e:
                # Filer might be rebooting
                time.sleep(5)
            retry = retry - 1

    def _update_volume_status(self):
        """Retrieve status info from volume group."""
        super(NetAppDirect7modeNfsDriver, self)._update_volume_status()
        netapp_backend = 'NetApp_NFS_7mode_direct'
        backend_name = self.configuration.safe_get('volume_backend_name')
        self._stats["volume_backend_name"] = (backend_name or
                                              'NetApp_NFS_7mode_direct')
        self._stats["vendor_name"] = 'NetApp'
        self._stats["driver_version"] = '1.0'
        provide_ems(self, self._client, self._stats, netapp_backend,
                    server_type="7mode")

    def _shortlist_del_eligible_files(self, share, old_files):
        """Prepares list of eligible files to be deleted from cache."""
        file_list = []
        exp_volume = self._get_actual_path_for_export(share)
        for file in old_files:
            path = '/vol/%s/%s' % (exp_volume, file)
            u_bytes = self._get_filer_file_usage(path)
            file_list.append((file, u_bytes))
        LOG.debug(_('Shortlisted del elg files %s'), file_list)
        return file_list

    def _get_filer_file_usage(self, path):
        """Gets the file unique bytes."""
        LOG.debug(_('Getting file usage for %s'), path)
        file_use = NaElement.create_node_with_children(
            'file-usage-get', **{'path': path})
        res = self._invoke_successfully(file_use)
        bytes = res.get_child_content('unique-bytes')
        LOG.debug(_('file-usage for path %(path)s is %(bytes)s')
            % {'path': path, 'bytes': bytes})
        return bytes

    def _is_filer_ip(self, ip):
        """Checks whether ip is on the same filer."""
        try:
            ifconfig = NaElement('net-ifconfig-get')
            res = self._invoke_successfully(ifconfig, None)
            if_info = res.get_child_by_name('interface-config-info')
            if if_info:
                ifs = if_info.get_children()
                for intf in ifs:
                    v4_addr = intf.get_child_by_name('v4-primary-address')
                    if v4_addr:
                        ip_info = v4_addr.get_child_by_name('ip-address-info')
                        if ip_info:
                            address = ip_info.get_child_content('address')
                            if ip == address:
                                return True
                            else:
                                continue
        except:
            return False
        return False

    def _share_match_for_ip(self, ip, shares):
        """Returns the share that is served by ip."""
        if self._is_filer_ip(ip) and shares:
            for share in shares:
                ip_sh = share.split(':')[0]
                if self._is_filer_ip(ip_sh):
                    LOG.debug(_('Share match found for ip %s'), ip)
                    return share
        LOG.debug(_('No share match found for ip %s'), ip)
        return None
