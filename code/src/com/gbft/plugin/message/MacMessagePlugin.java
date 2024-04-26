package com.gbft.plugin.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.gbft.framework.core.Entity;
import com.gbft.framework.data.MessageData;
import com.gbft.framework.data.PluginData;
import com.gbft.framework.plugins.InitializablePluginInterface;
import com.gbft.framework.plugins.MessagePlugin;
import com.gbft.framework.utils.DataUtils;
import com.gbft.framework.utils.EntityMapUtils;
import com.gbft.framework.utils.Printer;
import com.gbft.framework.utils.Printer.Verbosity;
import com.google.protobuf.ByteString;

public class MacMessagePlugin implements MessagePlugin, InitializablePluginInterface {

    private Entity entity;

    protected Map<Integer, byte[]> secretKeys;

    private boolean initialized;

    private static final int INIT = 0;
    private static final int SECRET_KEY = 101;
    private static final int MAC_VECTOR = 1001;

    public MacMessagePlugin(Entity entity) {
        this.entity = entity;

        initialized = false;
        secretKeys = new ConcurrentHashMap<>();
    }

    @Override
    public MessageData processIncomingMessage(MessageData message) {
        if (message.getFlagsList().contains(DataUtils.INVALID)) {
            System.out.println("Invalid message received");
            return message;
        }

        if (verifyMac(message)) {
            System.out.println("MAC validation successful");
            return message;
        }
        else{
            System.out.println("MAC validation failed");
        }

        Printer.print(Verbosity.VVV, entity.prefix, "Mac validation failed for ", message);
        return DataUtils.invalidate(message);
    }

    @Override
    public MessageData processOutgoingMessage(MessageData message) {
        var requestList = message.getRequestsList();
        var copyBuilder = message.toBuilder()
            .clearExtraData()
            .clearFault()       // clear fault
            .clearRequests();   // clear requests first
        // clean the dummy part in request
        for (var request : requestList) {
            copyBuilder.addRequests(request.toBuilder().clearRequestDummy().build());
        }
        message = copyBuilder.build();

        var bytes = message.toByteArray();

        var targets = message.getTargetsList();
        var macVector = generateMacVector(bytes, targets);

        return MessageData.newBuilder(message)
                          .putExtraData(MAC_VECTOR, ByteString.copyFrom(macVector))
                          .build();
    }

    @Override
    public void handleInitEvent(PluginData pluginData) {
        var messageType = pluginData.getMessageType();
        if (messageType == INIT) {
            KeyGenerator keygen = null;
            try {
                keygen = KeyGenerator.getInstance("HmacSHA512");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            var total = EntityMapUtils.nodeCount() + EntityMapUtils.clientCount();
            /*
             * Runs a loop from runner+1 to others
             */
            for (var target = entity.getId() + 1; target < total; target += 1) {
                keygen.init(256);
                SecretKey hmacKey = keygen.generateKey();
                System.out.println("Updating secret keys with value for target: "+target);
                secretKeys.put(target, hmacKey.getEncoded());

                var bytes = ByteString.copyFrom(hmacKey.getEncoded());
                var secretKeyData = DataUtils.createPluginData("mac", SECRET_KEY, bytes, target == 4 ? entity.getCoordinator().getMyUnit() : entity.getId(),
                        List.of(target));
                var secretKeyEvent = DataUtils.createEvent(secretKeyData);

                var unit = EntityMapUtils.getUnit(target);

                entity.getCoordinator().superSendEvent(unit, secretKeyEvent);
                Printer.print(Verbosity.VVVV, entity.prefix, "Sent secret key to " + target);
            }
        } else if (pluginData.getPluginName().equals("mac")) {
            if (messageType == SECRET_KEY) {
                var source = pluginData.getSource();
                var bytes = pluginData.getData().toByteArray();
                System.out.println("Updating secret keys with value for source: "+source);
                secretKeys.put(source, bytes);
            }
        }
        System.out.println("Secret Key size: "+secretKeys.size());

        if (secretKeys.size() == EntityMapUtils.nodeCount() + EntityMapUtils.clientCount() - 1) {
            initialized = true;
            Printer.print(Verbosity.VV, entity.prefix, "Secret key exchange completed.");
        }
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    private byte[] generateMacVector(byte[] data, List<Integer> targets) {
        var stream = new ByteArrayOutputStream();
        stream.write(targets.size());

        try {
            for (var target : targets) {
                if (target == entity.getId()) {
                    continue;
                }

                var mac = Mac.getInstance("HmacSHA512");
                mac.init(new SecretKeySpec(secretKeys.get(target), "HmacSHA512"));
                mac.update(data);
                var bytes = mac.doFinal();
                stream.write(target);
                stream.write(bytes.length);
                stream.write(bytes);
            }
        } catch (NoSuchAlgorithmException | InvalidKeyException | IOException e) {
            e.printStackTrace();
            return null;
        }

        return stream.toByteArray();
    }

    private byte[] generateMac(byte[] data, byte[] secretKey) {
        try {
            var mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(secretKey, "HmacSHA512"));
            mac.update(data);
            var bytes = mac.doFinal();
            return bytes;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            e.printStackTrace();
        }

        return null;
    }

    public boolean verifyMac(MessageData message) {
        var source = message.getSource();
        /*
         * In case of clusters, message from client will come from id 16 but
         * for each the client is stored at id 4
         */
        if(source == 16)
            source = 4;
        if (source == entity.getId()) {
            return true;
        }

        if (!message.containsExtraData(MAC_VECTOR)) {
            return false;
        }
        System.out.println("Verifying MAC for source: "+source);
        var secretKey = secretKeys.get(source);
        System.out.println("Secret Key: "+secretKey);
        var macData = message.getExtraDataOrThrow(MAC_VECTOR);

        var requestList = message.getRequestsList();
        var copyBuilder = message.toBuilder()
            .clearExtraData()
            .clearFault()       // clear fault
            .clearRequests();   // clear requests first
        // clean the dummy part in request
        for (var request : requestList) {
            copyBuilder.addRequests(request.toBuilder().clearRequestDummy().build());
        }
        var copy = copyBuilder.build();

        var computed = generateMac(copy.toByteArray(), secretKey);

        var stream = new ByteArrayInputStream(macData.toByteArray());
        var count = stream.read();
        for (var i = 0; i < count; i++) {
            var target = stream.read();
            var len = stream.read();
            byte[] bytes;
            try {
                bytes = stream.readNBytes(len);
                /*
                 * Adding this because message target is the client id but here
                 * we have defined to use runner value
                 */
                target = target%4;
                if (target == entity.getId()) {
                    return Arrays.equals(computed, bytes);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return false;
    }

}
