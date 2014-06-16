/*  Copyright 2013 Yahoo! Inc.                                         */
/*  See LICENSE in the root of the distribution for licensing details. */

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
extern datum getPageEntryKey(struct mdbm_shake_data_v3 *shakeinfo, uint32_t index);
extern datum getPageEntryValue(struct mdbm_shake_data_v3 *shakeinfo, uint32_t index);
extern uint32_t getCount(struct mdbm_shake_data_v3 *shakeinfo);
extern uint32_t getPageNum(struct mdbm_shake_data_v3 *shakeinfo);
extern uint32_t getFreeSpace(struct mdbm_shake_data_v3 *shakeinfo);
extern uint32_t getSpaceNeeded(struct mdbm_shake_data_v3 *shakeinfo);
extern void setEntryDeleted(struct mdbm_shake_data_v3 *shakeinfo, uint32_t index);

extern void AddPtr(void *ptr);
extern void *DeleteExistingPtr(void *ptr);  // Returns NULL if not found, ptr otherwise
#ifdef __cplusplus
}
#endif

