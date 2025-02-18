/* nbdkit
 * Copyright (C) 2013-2019 Red Hat Inc.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Red Hat nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RED HAT AND CONTRIBUTORS ''AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL RED HAT OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <config.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>

#include "internal.h"

/* We extend the generic backend struct with extra fields relating
 * to this filter.
 */
struct backend_filter {
  struct backend backend;
  struct nbdkit_filter filter;
};

/* Literally a backend + a connection pointer.  This is the
 * implementation of ‘void *nxdata’ in the filter API.
 */
struct b_conn {
  struct backend *b;
  struct connection *conn;
};

/* Note this frees the whole chain. */
static void
filter_free (struct backend *b)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);

  b->next->free (b->next);

  backend_unload (b, f->filter.unload);
  free (f);
}

static int
filter_thread_model (struct backend *b)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  int filter_thread_model = NBDKIT_THREAD_MODEL_PARALLEL;
  int thread_model = b->next->thread_model (b->next);

  if (f->filter.thread_model) {
    filter_thread_model = f->filter.thread_model ();
    if (filter_thread_model == -1)
      exit (EXIT_FAILURE);
  }

  if (filter_thread_model < thread_model) /* more serialized */
    thread_model = filter_thread_model;

  return thread_model;
}

/* This is actually passing the request through to the final plugin,
 * hence the function name.
 */
static const char *
plugin_name (struct backend *b)
{
  return b->next->plugin_name (b->next);
}

static const char *
filter_version (struct backend *b)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);

  return f->filter._version;
}

static void
filter_usage (struct backend *b)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  const char *p;

  printf ("filter: %s", b->name);
  if (f->filter.longname)
    printf (" (%s)", f->filter.longname);
  printf ("\n");
  printf ("(%s)\n", b->filename);
  if (f->filter.description) {
    printf ("%s", f->filter.description);
    if ((p = strrchr (f->filter.description, '\n')) == NULL || p[1])
      printf ("\n");
  }
  if (f->filter.config_help) {
    printf ("%s", f->filter.config_help);
    if ((p = strrchr (f->filter.config_help, '\n')) == NULL || p[1])
      printf ("\n");
  }
}

static void
filter_dump_fields (struct backend *b)
{
  b->next->dump_fields (b->next);
}

static int
next_config (void *nxdata, const char *key, const char *value)
{
  struct backend *b = nxdata;
  b->config (b, key, value);
  return 0;
}

static void
filter_config (struct backend *b, const char *key, const char *value)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);

  debug ("%s: config key=%s, value=%s",
         b->name, key, value);

  if (f->filter.config) {
    if (f->filter.config (next_config, b->next, key, value) == -1)
      exit (EXIT_FAILURE);
  }
  else
    b->next->config (b->next, key, value);
}

static int
next_config_complete (void *nxdata)
{
  struct backend *b = nxdata;
  b->config_complete (b);
  return 0;
}

static void
filter_config_complete (struct backend *b)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);

  debug ("%s: config_complete", b->name);

  if (f->filter.config_complete) {
    if (f->filter.config_complete (next_config_complete, b->next) == -1)
      exit (EXIT_FAILURE);
  }
  else
    b->next->config_complete (b->next);
}

/* magic_config_key only applies to plugins, so this passes the
 * request through to the plugin (hence the name).
 */
static const char *
plugin_magic_config_key (struct backend *b)
{
  return b->next->magic_config_key (b->next);
}

static int
next_open (void *nxdata, int readonly)
{
  struct b_conn *b_conn = nxdata;

  return backend_open (b_conn->b, b_conn->conn, readonly);
}

static void *
filter_open (struct backend *b, struct connection *conn, int readonly)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  /* Most filters will call next_open first, resulting in
   * inner-to-outer ordering.
   */
  if (f->filter.open)
    return f->filter.open (next_open, &nxdata, readonly);
  else if (backend_open (b->next, conn, readonly) == -1)
    return NULL;
  else
    return NBDKIT_HANDLE_NOT_NEEDED;
}

static void
filter_close (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);

  if (handle && f->filter.close)
    f->filter.close (handle);
}

/* The next_functions structure contains pointers to backend
 * functions.  However because these functions are all expecting a
 * backend and a connection, we cannot call them directly, but must
 * write some next_* functions that unpack the two parameters from a
 * single ‘void *nxdata’ struct pointer (‘b_conn’).
 */

static int
next_reopen (void *nxdata, int readonly)
{
  struct b_conn *b_conn = nxdata;
  return backend_reopen (b_conn->b, b_conn->conn, readonly);
}

static int64_t
next_get_size (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_get_size (b_conn->b, b_conn->conn);
}

static int
next_can_write (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_write (b_conn->b, b_conn->conn);
}

static int
next_can_flush (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_flush (b_conn->b, b_conn->conn);
}

static int
next_is_rotational (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_is_rotational (b_conn->b, b_conn->conn);
}

static int
next_can_trim (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_trim (b_conn->b, b_conn->conn);
}

static int
next_can_zero (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_zero (b_conn->b, b_conn->conn);
}

static int
next_can_fast_zero (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_fast_zero (b_conn->b, b_conn->conn);
}

static int
next_can_extents (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_extents (b_conn->b, b_conn->conn);
}

static int
next_can_fua (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_fua (b_conn->b, b_conn->conn);
}

static int
next_can_multi_conn (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_multi_conn (b_conn->b, b_conn->conn);
}

static int
next_can_cache (void *nxdata)
{
  struct b_conn *b_conn = nxdata;
  return backend_can_cache (b_conn->b, b_conn->conn);
}

static int
next_pread (void *nxdata, void *buf, uint32_t count, uint64_t offset,
            uint32_t flags, int *err)
{
  struct b_conn *b_conn = nxdata;
  return backend_pread (b_conn->b, b_conn->conn, buf, count, offset, flags,
                        err);
}

static int
next_pwrite (void *nxdata, const void *buf, uint32_t count, uint64_t offset,
             uint32_t flags, int *err)
{
  struct b_conn *b_conn = nxdata;
  return backend_pwrite (b_conn->b, b_conn->conn, buf, count, offset, flags,
                         err);
}

static int
next_flush (void *nxdata, uint32_t flags, int *err)
{
  struct b_conn *b_conn = nxdata;
  return backend_flush (b_conn->b, b_conn->conn, flags, err);
}

static int
next_trim (void *nxdata, uint32_t count, uint64_t offset, uint32_t flags,
           int *err)
{
  struct b_conn *b_conn = nxdata;
  return backend_trim (b_conn->b, b_conn->conn, count, offset, flags, err);
}

static int
next_zero (void *nxdata, uint32_t count, uint64_t offset, uint32_t flags,
           int *err)
{
  struct b_conn *b_conn = nxdata;
  return backend_zero (b_conn->b, b_conn->conn, count, offset, flags, err);
}

static int
next_extents (void *nxdata, uint32_t count, uint64_t offset, uint32_t flags,
              struct nbdkit_extents *extents, int *err)
{
  struct b_conn *b_conn = nxdata;
  return backend_extents (b_conn->b, b_conn->conn, count, offset, flags,
                          extents, err);
}

static int
next_cache (void *nxdata, uint32_t count, uint64_t offset,
            uint32_t flags, int *err)
{
  struct b_conn *b_conn = nxdata;
  return backend_cache (b_conn->b, b_conn->conn, count, offset, flags, err);
}

static struct nbdkit_next_ops next_ops = {
  .reopen = next_reopen,
  .get_size = next_get_size,
  .can_write = next_can_write,
  .can_flush = next_can_flush,
  .is_rotational = next_is_rotational,
  .can_trim = next_can_trim,
  .can_zero = next_can_zero,
  .can_fast_zero = next_can_fast_zero,
  .can_extents = next_can_extents,
  .can_fua = next_can_fua,
  .can_multi_conn = next_can_multi_conn,
  .can_cache = next_can_cache,
  .pread = next_pread,
  .pwrite = next_pwrite,
  .flush = next_flush,
  .trim = next_trim,
  .zero = next_zero,
  .extents = next_extents,
  .cache = next_cache,
};

static int
filter_prepare (struct backend *b, struct connection *conn, void *handle,
                int readonly)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.prepare &&
      f->filter.prepare (&next_ops, &nxdata, handle, readonly) == -1)
    return -1;

  return 0;
}

static int
filter_finalize (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.finalize &&
      f->filter.finalize (&next_ops, &nxdata, handle) == -1)
    return -1;
  return 0;
}

static int64_t
filter_get_size (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.get_size)
    return f->filter.get_size (&next_ops, &nxdata, handle);
  else
    return backend_get_size (b->next, conn);
}

static int
filter_can_write (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_write)
    return f->filter.can_write (&next_ops, &nxdata, handle);
  else
    return backend_can_write (b->next, conn);
}

static int
filter_can_flush (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_flush)
    return f->filter.can_flush (&next_ops, &nxdata, handle);
  else
    return backend_can_flush (b->next, conn);
}

static int
filter_is_rotational (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.is_rotational)
    return f->filter.is_rotational (&next_ops, &nxdata, handle);
  else
    return backend_is_rotational (b->next, conn);
}

static int
filter_can_trim (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_trim)
    return f->filter.can_trim (&next_ops, &nxdata, handle);
  else
    return backend_can_trim (b->next, conn);
}

static int
filter_can_zero (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_zero)
    return f->filter.can_zero (&next_ops, &nxdata, handle);
  else
    return backend_can_zero (b->next, conn);
}

static int
filter_can_fast_zero (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_fast_zero)
    return f->filter.can_fast_zero (&next_ops, &nxdata, handle);
  else
    return backend_can_fast_zero (b->next, conn);
}

static int
filter_can_extents (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_extents)
    return f->filter.can_extents (&next_ops, &nxdata, handle);
  else
    return backend_can_extents (b->next, conn);
}

static int
filter_can_fua (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_fua)
    return f->filter.can_fua (&next_ops, &nxdata, handle);
  else
    return backend_can_fua (b->next, conn);
}

static int
filter_can_multi_conn (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_multi_conn)
    return f->filter.can_multi_conn (&next_ops, &nxdata, handle);
  else
    return backend_can_multi_conn (b->next, conn);
}

static int
filter_can_cache (struct backend *b, struct connection *conn, void *handle)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.can_cache)
    return f->filter.can_cache (&next_ops, &nxdata, handle);
  else
    return backend_can_cache (b->next, conn);
}

static int
filter_pread (struct backend *b, struct connection *conn, void *handle,
              void *buf, uint32_t count, uint64_t offset,
              uint32_t flags, int *err)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.pread)
    return f->filter.pread (&next_ops, &nxdata, handle,
                            buf, count, offset, flags, err);
  else
    return backend_pread (b->next, conn, buf, count, offset, flags, err);
}

static int
filter_pwrite (struct backend *b, struct connection *conn, void *handle,
               const void *buf, uint32_t count, uint64_t offset,
               uint32_t flags, int *err)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.pwrite)
    return f->filter.pwrite (&next_ops, &nxdata, handle,
                             buf, count, offset, flags, err);
  else
    return backend_pwrite (b->next, conn, buf, count, offset, flags, err);
}

static int
filter_flush (struct backend *b, struct connection *conn, void *handle,
              uint32_t flags, int *err)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.flush)
    return f->filter.flush (&next_ops, &nxdata, handle, flags, err);
  else
    return backend_flush (b->next, conn, flags, err);
}

static int
filter_trim (struct backend *b, struct connection *conn, void *handle,
             uint32_t count, uint64_t offset,
             uint32_t flags, int *err)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.trim)
    return f->filter.trim (&next_ops, &nxdata, handle, count, offset, flags,
                           err);
  else
    return backend_trim (b->next, conn, count, offset, flags, err);
}

static int
filter_zero (struct backend *b, struct connection *conn, void *handle,
             uint32_t count, uint64_t offset, uint32_t flags, int *err)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.zero)
    return f->filter.zero (&next_ops, &nxdata, handle,
                           count, offset, flags, err);
  else
    return backend_zero (b->next, conn, count, offset, flags, err);
}

static int
filter_extents (struct backend *b, struct connection *conn, void *handle,
                uint32_t count, uint64_t offset, uint32_t flags,
                struct nbdkit_extents *extents, int *err)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };

  if (f->filter.extents)
    return f->filter.extents (&next_ops, &nxdata, handle,
                              count, offset, flags,
                              extents, err);
  else
    return backend_extents (b->next, conn, count, offset, flags,
                            extents, err);
}

static int
filter_cache (struct backend *b, struct connection *conn, void *handle,
              uint32_t count, uint64_t offset,
              uint32_t flags, int *err)
{
  struct backend_filter *f = container_of (b, struct backend_filter, backend);
  struct b_conn nxdata = { .b = b->next, .conn = conn };


  if (f->filter.cache)
    return f->filter.cache (&next_ops, &nxdata, handle,
                            count, offset, flags, err);
  else
    return backend_cache (b->next, conn, count, offset, flags, err);
}

static struct backend filter_functions = {
  .free = filter_free,
  .thread_model = filter_thread_model,
  .plugin_name = plugin_name,
  .usage = filter_usage,
  .version = filter_version,
  .dump_fields = filter_dump_fields,
  .config = filter_config,
  .config_complete = filter_config_complete,
  .magic_config_key = plugin_magic_config_key,
  .open = filter_open,
  .prepare = filter_prepare,
  .finalize = filter_finalize,
  .close = filter_close,
  .get_size = filter_get_size,
  .can_write = filter_can_write,
  .can_flush = filter_can_flush,
  .is_rotational = filter_is_rotational,
  .can_trim = filter_can_trim,
  .can_zero = filter_can_zero,
  .can_fast_zero = filter_can_fast_zero,
  .can_extents = filter_can_extents,
  .can_fua = filter_can_fua,
  .can_multi_conn = filter_can_multi_conn,
  .can_cache = filter_can_cache,
  .pread = filter_pread,
  .pwrite = filter_pwrite,
  .flush = filter_flush,
  .trim = filter_trim,
  .zero = filter_zero,
  .extents = filter_extents,
  .cache = filter_cache,
};

/* Register and load a filter. */
struct backend *
filter_register (struct backend *next, size_t index, const char *filename,
                 void *dl, struct nbdkit_filter *(*filter_init) (void))
{
  struct backend_filter *f;
  const struct nbdkit_filter *filter;

  f = calloc (1, sizeof *f);
  if (f == NULL) {
    perror ("strdup");
    exit (EXIT_FAILURE);
  }

  f->backend = filter_functions;
  backend_init (&f->backend, next, index, filename, dl, "filter");

  /* Call the initialization function which returns the address of the
   * filter's own 'struct nbdkit_filter'.
   */
  filter = filter_init ();
  if (!filter) {
    fprintf (stderr, "%s: %s: filter registration function failed\n",
             program_name, filename);
    exit (EXIT_FAILURE);
  }

  /* We do not provide API or ABI guarantees for filters, other than
   * the ABI position and API contents of _api_version and _version to
   * diagnose mismatch from the current nbdkit version.
   */
  if (filter->_api_version != NBDKIT_FILTER_API_VERSION) {
    fprintf (stderr,
             "%s: %s: filter is incompatible with this version of nbdkit "
             "(_api_version = %d, need %d)\n",
             program_name, filename, filter->_api_version,
             NBDKIT_FILTER_API_VERSION);
    exit (EXIT_FAILURE);
  }
  if (filter->_version == NULL ||
      strcmp (filter->_version, PACKAGE_VERSION) != 0) {
    fprintf (stderr,
             "%s: %s: filter is incompatible with this version of nbdkit "
             "(_version = %s, need %s)\n",
             program_name, filename, filter->_version ?: "<null>",
             PACKAGE_VERSION);
    exit (EXIT_FAILURE);
  }

  f->filter = *filter;

  backend_load (&f->backend, f->filter.name, f->filter.load);

  return (struct backend *) f;
}
