#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct albedo_bucket albedo_bucket;
typedef struct albedo_list_handle albedo_list_handle;
typedef struct albedo_transform_iterator albedo_transform_iterator;
typedef struct albedo_subscription_handle albedo_subscription_handle;

typedef enum albedo_result {
  ALBEDO_OK = 0,
  ALBEDO_ERROR = 1,
  ALBEDO_HAS_DATA = 2,
  ALBEDO_EOS = 3,
  ALBEDO_OUT_OF_MEMORY = 4,
  ALBEDO_FILE_NOT_FOUND = 5,
  ALBEDO_NOT_FOUND = 6,
  ALBEDO_INVALID_FORMAT = 7,
  ALBEDO_DUPLICATE_KEY = 8,
  ALBEDO_INVALID_CURSOR = 9,
  ALBEDO_UNSUPPORTED_CURSOR_QUERY = 10,
  ALBEDO_OPLOG_GAP = 11,
} albedo_result;

typedef enum albedo_op_kind {
  ALBEDO_OP_INSERT = 1,
  ALBEDO_OP_UPDATE = 2,
  ALBEDO_OP_DELETE = 3,
} albedo_op_kind;

typedef enum albedo_payload_kind {
  ALBEDO_PAYLOAD_INLINE = 0,
  ALBEDO_PAYLOAD_REF = 1,
  ALBEDO_PAYLOAD_NONE = 2,
} albedo_payload_kind;

typedef uint8_t (*albedo_page_change_callback)(
    void *context,
    const uint8_t *data,
    uint32_t data_size,
    uint32_t page_count);

albedo_result albedo_open(char *path, albedo_bucket **out);
albedo_result albedo_open_with_options(char *path, uint8_t *options_buffer, albedo_bucket **out);
albedo_result albedo_close(albedo_bucket *bucket);

albedo_result albedo_insert(albedo_bucket *bucket, uint8_t *doc_buffer);
albedo_result albedo_ensure_index(albedo_bucket *bucket, const char *path, uint8_t options_byte);
albedo_result albedo_drop_index(albedo_bucket *bucket, const char *path);
albedo_result albedo_list_indexes(albedo_bucket *bucket, uint8_t **out_doc);
albedo_result albedo_delete(albedo_bucket *bucket, uint8_t *query_buffer, uint16_t query_len);

albedo_result albedo_list(albedo_bucket *bucket, uint8_t *query_buffer, albedo_list_handle **out_iterator);
albedo_result albedo_list_cursor_export(albedo_list_handle *handle, uint8_t **out_cursor);
albedo_result albedo_data(albedo_list_handle *handle, uint8_t **out_doc);
albedo_result albedo_next(albedo_list_handle *handle);
albedo_result albedo_close_iterator(albedo_list_handle *iterator);

albedo_result albedo_vacuum(albedo_bucket *bucket);
albedo_result albedo_flush(albedo_bucket *bucket);

albedo_result albedo_transform(
    albedo_bucket *bucket,
    uint8_t *query_buffer,
    albedo_transform_iterator **iterator_out);
albedo_result albedo_transform_data(albedo_transform_iterator *iterator, uint8_t **out_doc);
albedo_result albedo_transform_apply(albedo_transform_iterator *iterator, uint8_t *transform_buffer);
albedo_result albedo_transform_close(albedo_transform_iterator *iterator);

albedo_result albedo_set_replication_callback(
    albedo_bucket *bucket,
    albedo_page_change_callback callback,
    void *context);
albedo_result albedo_apply_batch(
    albedo_bucket *bucket,
    const uint8_t *data,
    uint32_t data_size,
    uint32_t page_count);

uint32_t albedo_bitsize(void);
uint32_t albedo_version(void);

uint8_t *albedo_malloc(size_t size);
void albedo_free(uint8_t *ptr, size_t size);

/* ── Subscription (oplog change stream) ──────────────────────────────── */

albedo_result albedo_subscribe(
    albedo_bucket *bucket,
    uint8_t *query_buffer,
    albedo_subscription_handle **out_handle);

/// Poll for new change events.
///
/// On ALBEDO_HAS_DATA, *out_doc points to a BSON document
/// `{batch: [{seqno, op, doc_id, ts, doc?}, ...]}`.
/// The memory is owned by the subscription and stays valid until the
/// next albedo_subscribe_poll() or albedo_subscribe_close() call.
///
/// Returns ALBEDO_EOS when idle, ALBEDO_OPLOG_GAP when the subscriber
/// fell behind (must re-subscribe).
albedo_result albedo_subscribe_poll(
    albedo_subscription_handle *handle,
    uint8_t **out_doc,
    uint32_t max_events);

uint64_t albedo_subscribe_seqno(albedo_subscription_handle *handle);

albedo_result albedo_subscribe_close(albedo_subscription_handle *handle);

#ifdef __cplusplus
} // extern "C"
#endif
