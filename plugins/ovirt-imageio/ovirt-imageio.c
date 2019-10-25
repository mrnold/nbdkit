#define NBDKIT_API_VERSION 2

#include <curl/curl.h>
#include <nbdkit-plugin.h>

#define THREAD_MODEL NBDKIT_THREAD_MODEL_PARALLEL

static void *
ovirt_imageio_open (int readonly)
{
  static int* handle;
  nbdkit_debug ("Called ovirt_imageio_open");
  return (void *)handle;
}

static void
ovirt_imageio_load (void)
{
  nbdkit_debug ("Called ovirt_imageio_load");
  curl_global_init (CURL_GLOBAL_DEFAULT);
}

static void
ovirt_imageio_unload (void)
{
  nbdkit_debug ("Called ovirt_imageio_unload");
  curl_global_cleanup ();
}

static int64_t
ovirt_imageio_get_size (void *handle)
{
  handle;
  nbdkit_debug ("Called ovirt_imageio_get_size");
}

static int
ovirt_imageio_pread (void *handle, void *bug, uint32_t count,
                     uint64_t offset, uint32_t flags)
{
  nbdkit_debug ("Call ovirt_imageio_pread");
}

static struct nbdkit_plugin plugin = {
  .name              = "ovirtapi", // No special characters allowed
  .load              = ovirt_imageio_load,
  .open              = ovirt_imageio_open,
  .get_size          = ovirt_imageio_get_size,
  .pread             = ovirt_imageio_pread
};

NBDKIT_REGISTER_PLUGIN(plugin)
