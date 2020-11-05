package feed;

public class SensorReading {
    private Integer machineId = null;
    private long voltage;

    public int getMachineId() { return machineId; }

    public void setMachineId(int id) { this.machineId = id; }

    public long getVoltage() { return voltage; }

    public void setVoltage(long voltage) { this.voltage = voltage; }

    public String toString() {
        return "{ \"machine_id\": \"" + machineId + "\", " +
                "\"voltage\": \"" + voltage + "\" }";
    }

}
