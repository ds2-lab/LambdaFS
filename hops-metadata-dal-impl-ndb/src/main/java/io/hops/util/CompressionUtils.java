/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressionUtils {

  public static byte[] compress(byte[] data) throws IOException {
    Deflater compresser = new Deflater(Deflater.BEST_COMPRESSION);
    compresser.setInput(data);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
    compresser.finish();
    byte[] buffer = new byte[1024];
    while (!compresser.finished()) {
      int count = compresser.deflate(buffer);
      outputStream.write(buffer, 0, count);
    }
    compresser.end();
    outputStream.close();
    return outputStream.toByteArray();
  }

  public static byte[] decompress(byte[] data)
          throws IOException, DataFormatException {
    if (data != null && data.length != 0) {
      Inflater decompresser = new Inflater();
      decompresser.setInput(data);
      ByteArrayOutputStream outputStream
              = new ByteArrayOutputStream(data.length);
      byte[] buffer = new byte[1024];
      while (!decompresser.finished()) {
        int count = decompresser.inflate(buffer);
        outputStream.write(buffer, 0, count);
      }
      decompresser.end();
      outputStream.close();
      return outputStream.toByteArray();
    } else {
      return null;
    }
  }
}
