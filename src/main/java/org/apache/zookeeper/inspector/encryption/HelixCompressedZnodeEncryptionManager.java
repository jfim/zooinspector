/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.inspector.encryption;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 *
 */
public class HelixCompressedZnodeEncryptionManager implements DataEncryptionManager {

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.zookeeper.inspector.encryption.DataEncryptionManager#decryptData
     * (byte[])
     */
    @Override
    public String decryptData(byte[] encrypted) throws Exception {
        if (encrypted == null) {
            return null;
        }

        // Check for gzip header
        if (encrypted[0] == 0x1F && encrypted[1] == (byte) 0x8B) {
            GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(encrypted));
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int byteRead = inputStream.read();

            while (byteRead != -1) {
                outputStream.write(byteRead);
                byteRead = inputStream.read();
            }

            return new String(outputStream.toByteArray());
        }

        return new String(encrypted);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.zookeeper.inspector.encryption.DataEncryptionManager#encryptData
     * (java.lang.String)
     */
    @Override
    public byte[] encryptData(String data) throws Exception {
        if (data == null) {
            return new byte[0];
        }

        // If it's a helix compressed znode, compress it back
        if (data.contains("\"enableCompression\":\"true\"")) {
            final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            GZIPOutputStream outputStream = new GZIPOutputStream(byteOutputStream);
            byte[] bytes = data.getBytes();
            outputStream.write(bytes);
            outputStream.close();
            final byte[] resultingBytes = byteOutputStream.toByteArray();
            return resultingBytes;
        }

        return data.getBytes();
    }

}
