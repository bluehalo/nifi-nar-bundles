package com.asymmetrik.nifi.processors.util;
/*
 *  Copyright 2011, 2012 Martin Matula (martin@alutam.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * @author Martin Matula (martin at alutam.com)
 */
public class ZipUtil {
    static final int[] CRC_TABLE = new int[256];
    static final int DECRYPT_HEADER_SIZE = 12;
    static final int[] CFH_SIGNATURE = {0x50, 0x4b, 0x01, 0x02};
    static final int[] LFH_SIGNATURE = {0x50, 0x4b, 0x03, 0x04};
    static final int[] ECD_SIGNATURE = {0x50, 0x4b, 0x05, 0x06};
    static final int[] DD_SIGNATURE = {0x50, 0x4b, 0x07, 0x08};

    // compute the table
    // (could also have it pre-computed - see http://snippets.dzone.com/tag/crc32)
    static {
        for (int i = 0; i < 256; i++) {
            int r = i;
            for (int j = 0; j < 8; j++) {
                if ((r & 1) == 1) {
                    r = (r >>> 1) ^ 0xedb88320;
                } else {
                    r >>>= 1;
                }
            }
            CRC_TABLE[i] = r;
        }
    }

    static void updateKeys(byte charAt, int[] keys) {
        keys[0] = crc32(keys[0], charAt);
        keys[1] += keys[0] & 0xff;
        keys[1] = keys[1] * 134775813 + 1;
        keys[2] = crc32(keys[2], (byte) (keys[1] >> 24));
    }

    static int crc32(int oldCrc, byte charAt) {
        return ((oldCrc >>> 8) ^ CRC_TABLE[(oldCrc ^ charAt) & 0xff]);
    }

    static enum State {
        SIGNATURE, FLAGS, COMPRESSED_SIZE, FN_LENGTH, EF_LENGTH, HEADER, DATA, TAIL, CRC
    }

    static enum Section {
        FILE_HEADER, FILE_DATA, DATA_DESCRIPTOR
    }
}
