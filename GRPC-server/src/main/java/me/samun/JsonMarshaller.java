/*
 * Copyright 2018 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.samun;



import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * A {@link Marshaller} for JSON.  This marshals in the Protobuf 3 format described here:
 * https://developers.google.com/protocol-buffers/docs/proto3#json
 */
final class JsonMarshaller {

  private JsonMarshaller() {}

  /**
   * Create a {@code Marshaller} for json protos of the same type as {@code defaultInstance}.
   *
   * <p>This is an unstable API and has not been optimized yet for performance.
   */
  public static <T extends Message> MethodDescriptor.Marshaller<T> jsonMarshaller(final T defaultInstance) {
    final JsonFormat.Parser parser = JsonFormat.parser();
    final JsonFormat.Printer printer = com.google.protobuf.util.JsonFormat.printer();
    return jsonMarshaller(defaultInstance, parser, printer);
  }

  /**
   * Create a {@code Marshaller} for json protos of the same type as {@code defaultInstance}.
   *
   * <p>This is an unstable API and has not been optimized yet for performance.
   */
  public static <T extends Message> MethodDescriptor.Marshaller<T> jsonMarshaller(
          final T defaultInstance, final JsonFormat.Parser parser, final JsonFormat.Printer printer) {

    final Charset charset = Charset.forName("UTF-8");

    return new MethodDescriptor.Marshaller<T>() {
      @Override
      public InputStream stream(T value) {
        try {
          return new ByteArrayInputStream(printer.print(value).getBytes(charset));
        } catch (InvalidProtocolBufferException e) {
          throw Status.INTERNAL
                  .withCause(e)
                  .withDescription("Unable to print json proto")
                  .asRuntimeException();
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public T parse(InputStream stream) {
        Message.Builder builder = defaultInstance.newBuilderForType();
        Reader reader = new InputStreamReader(stream, charset);
        T proto;
        try {
          parser.merge(reader, builder);
          proto = (T) builder.build();
          reader.close();
        } catch (InvalidProtocolBufferException e) {
          throw Status.INTERNAL.withDescription("Invalid protobuf byte sequence")
                  .withCause(e).asRuntimeException();
        } catch (IOException e) {
          // Same for now, might be unavailable
          throw Status.INTERNAL.withDescription("Invalid protobuf byte sequence")
                  .withCause(e).asRuntimeException();
        }
        return proto;
      }
    };
  }
}