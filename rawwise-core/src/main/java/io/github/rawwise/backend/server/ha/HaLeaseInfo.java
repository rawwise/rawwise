package io.github.rawwise.backend.server.ha;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.Objects;

/**
 * BE Server Active-Standby leader election lease info (persisted to Metastore).
 *
 * <p>Metastore key: {@code rawwise/cluster/active-server}
 *
 * <p>Leader election semantics:
 *
 * <ul>
 *   <li>initial no record -> CAS NotExists acquires leadership, epoch=1
 *   <li>record exists but expired -> CAS ValueEquals(oldSerialized) acquires leadership, epoch=old
 *       epoch+1
 *   <li>record exists and not expired -> acquire fails, enter Standby mode
 *   <li>holder renews -> CAS ValueEquals(currentSerialized) updates expireAtMs, epoch unchanged
 * </ul>
 *
 * <p>Split-Brain protection: epoch is unique. BE Nodes maintain {@code acceptedEpoch}, commands
 * with epoch < acceptedEpoch are rejected. Standby acquires leadership with epoch+1, BE Nodes
 * accept new Active.
 *
 * @param ownerId BE Server instance ID (uniquely identifies the BE Server process)
 * @param address external service address (host:port, for Frontend connection)
 * @param epoch lease epoch (unique; increments on leadership acquire, unchanged on renew)
 * @param expireAtMs lease expiry time (epoch millis)
 * @param leaseVersion lease version number (CAS lock, increments on modify)
 */
public record HaLeaseInfo(
        String ownerId, String address, long epoch, long expireAtMs, long leaseVersion) {

    private static final ObjectMapper MAPPER = createMapper();

    public HaLeaseInfo {
        Objects.requireNonNull(ownerId, "ownerId must not be null");
        Objects.requireNonNull(address, "address must not be null");
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be >= 0, got: " + epoch);
        }
        if (leaseVersion < 0) {
            throw new IllegalArgumentException("leaseVersion must be >= 0, got: " + leaseVersion);
        }
    }

    /**
     * Check if the lease has expired.
     *
     * @param nowMs current time (epoch millis)
     * @return true if lease has expired
     */
    public boolean isExpired(long nowMs) {
        return nowMs >= expireAtMs;
    }

    /**
     * Create a renewed lease (epoch unchanged, expireAtMs and leaseVersion updated).
     *
     * @param newExpireAtMs new expiry time
     * @return renewed HaLeaseInfo
     */
    public HaLeaseInfo renew(long newExpireAtMs) {
        return new HaLeaseInfo(ownerId, address, epoch, newExpireAtMs, leaseVersion + 1);
    }

    /**
     * Acquire leadership: new ownerId takes over, epoch+1.
     *
     * @param newOwnerId new owner ID
     * @param newAddress new external address
     * @param newExpireAtMs new expiry time
     * @return HaLeaseInfo after leadership acquire
     */
    public HaLeaseInfo takeOver(String newOwnerId, String newAddress, long newExpireAtMs) {
        return new HaLeaseInfo(newOwnerId, newAddress, epoch + 1, newExpireAtMs, leaseVersion + 1);
    }

    /**
     * Serialize to JSON bytes (used for Metastore CAS ValueEquals comparison).
     *
     * @return JSON bytes
     */
    public byte[] serialize() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize HaLeaseInfo: " + e.getMessage(), e);
        }
    }

    /**
     * Deserialize from JSON bytes.
     *
     * @param data JSON bytes
     * @return HaLeaseInfo
     */
    public static HaLeaseInfo deserialize(byte[] data) {
        try {
            return MAPPER.readValue(data, HaLeaseInfo.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize HaLeaseInfo: " + e.getMessage(), e);
        }
    }

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(
                com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        return mapper;
    }
}
