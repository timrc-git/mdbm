package com.yahoo.db.mdbm;

import static com.yahoo.db.mdbm.TestSimpleMdbm.testMdbmV3Path;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPool {

	@Test
	public void testEmptyFirst() throws Exception {
		MdbmPoolInterface mdbmPool = null;
		try {
			mdbmPool = MdbmProvider.openPool(testMdbmV3Path,
					Open.MDBM_O_RDONLY, 0755, 0, 0, 5);
			try (MdbmInterface handle = mdbmPool.getMdbmHandle()) {
				String key = "testkey";
				MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
				MdbmDatum value = handle.fetch(datum);
				Assert.assertNull(value);
				mdbmPool.returnMdbmHandle(handle);
			}

		} finally {
			if (null != mdbmPool) {
				mdbmPool.close();
			}
		}
	}
}
