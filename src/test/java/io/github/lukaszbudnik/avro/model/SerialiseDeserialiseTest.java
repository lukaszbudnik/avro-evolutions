package io.github.lukaszbudnik.avro.model;


import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class SerialiseDeserialiseTest {

    @Test
    public void shouldSerialiseDeserialiseAsBinaryTest() throws IOException {
        byte[] array = new byte[1024];
        new Random().nextBytes(array);

        ByteBuffer buffer = ByteBuffer.wrap(array);

        TestCaseV1 testCaseV1 = TestCaseV1.newBuilder()
                .setName("test case v1")
                .setContent(buffer)
                .build();

        TestRunV1 testRunV1 = TestRunV1.newBuilder()
                .setName("test run v1")
                .setConcurrencyLevel(2134567890)
                .setTestCases(Arrays.asList(testCaseV1))
                .build();

        DatumWriter<TestRunV1> writer = new SpecificDatumWriter<TestRunV1>(TestRunV1.getClassSchema());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder out = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(testRunV1, out);
        out.flush();

        System.out.println(baos.toByteArray().length);

        DatumReader<TestRunV2> reader = new SpecificDatumReader<TestRunV2>(TestRunV1.getClassSchema(), TestRunV2.getClassSchema());
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Decoder in = DecoderFactory.get().binaryDecoder(bais, null);
        TestRunV2 testRunV2 = reader.read(new TestRunV2(), in);

        Assert.assertEquals(testRunV1.getName(), testRunV2.getName());
        Assert.assertEquals(testRunV1.getTestCases(), testRunV2.getTestCases());
        Assert.assertEquals(buffer, testRunV2.getTestCases().get(0).getContent());
        Assert.assertNull(testRunV2.getTimezone());
    }

}
