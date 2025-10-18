package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.neuralchilli.quorch.domain.*;

import java.io.IOException;
import java.util.*;

/**
 * Custom Hazelcast serializers for domain objects.
 * Required because Hazelcast's automatic serialization can't handle Object-typed fields.
 */
public class HazelcastSerializers {

    /**
     * Serializer for Parameter records (contains Object defaultValue)
     */
    public static class ParameterSerializer implements StreamSerializer<Parameter> {
        private static final int TYPE_ID = 1;

        @Override
        public void write(ObjectDataOutput out, Parameter param) throws IOException {
            out.writeString(param.type().name());
            writeObject(out, param.defaultValue());
            out.writeBoolean(param.required());
            out.writeString(param.description());
        }

        @Override
        public Parameter read(ObjectDataInput in) throws IOException {
            ParameterType type = ParameterType.valueOf(in.readString());
            Object defaultValue = readObject(in);
            boolean required = in.readBoolean();
            String description = in.readString();

            return new Parameter(type, defaultValue, required, description);
        }

        @Override
        public int getTypeId() {
            return TYPE_ID;
        }

        @Override
        public void destroy() {
        }

        private void writeObject(ObjectDataOutput out, Object obj) throws IOException {
            if (obj == null) {
                out.writeByte(0);
            } else if (obj instanceof String) {
                out.writeByte(1);
                out.writeString((String) obj);
            } else if (obj instanceof Integer) {
                out.writeByte(2);
                out.writeInt((Integer) obj);
            } else if (obj instanceof Boolean) {
                out.writeByte(3);
                out.writeBoolean((Boolean) obj);
            } else if (obj instanceof Long) {
                out.writeByte(4);
                out.writeLong((Long) obj);
            } else if (obj instanceof Double) {
                out.writeByte(5);
                out.writeDouble((Double) obj);
            } else {
                // Fallback to string representation
                out.writeByte(1);
                out.writeString(obj.toString());
            }
        }

        private Object readObject(ObjectDataInput in) throws IOException {
            byte type = in.readByte();
            return switch (type) {
                case 0 -> null;
                case 1 -> in.readString();
                case 2 -> in.readInt();
                case 3 -> in.readBoolean();
                case 4 -> in.readLong();
                case 5 -> in.readDouble();
                default -> throw new IOException("Unknown object type: " + type);
            };
        }
    }

    /**
     * Serializer for TaskReference records (contains Map<String, Object> params)
     */
    public static class TaskReferenceSerializer implements StreamSerializer<TaskReference> {
        private static final int TYPE_ID = 2;

        @Override
        public void write(ObjectDataOutput out, TaskReference ref) throws IOException {
            // Write taskName (nullable)
            out.writeString(ref.taskName());

            // Write inlineTask (nullable)
            out.writeBoolean(ref.inlineTask() != null);
            if (ref.inlineTask() != null) {
                writeTask(out, ref.inlineTask());
            }

            // Write params map
            writeObjectMap(out, ref.params());

            // Write dependsOn list
            out.writeInt(ref.dependsOn().size());
            for (String dep : ref.dependsOn()) {
                out.writeString(dep);
            }
        }

        @Override
        public TaskReference read(ObjectDataInput in) throws IOException {
            String taskName = in.readString();

            Task inlineTask = null;
            if (in.readBoolean()) {
                inlineTask = readTask(in);
            }

            Map<String, Object> params = readObjectMap(in);

            int depsSize = in.readInt();
            List<String> dependsOn = new ArrayList<>(depsSize);
            for (int i = 0; i < depsSize; i++) {
                dependsOn.add(in.readString());
            }

            return new TaskReference(taskName, inlineTask, params, dependsOn);
        }

        @Override
        public int getTypeId() {
            return TYPE_ID;
        }

        @Override
        public void destroy() {
        }

        private void writeTask(ObjectDataOutput out, Task task) throws IOException {
            out.writeString(task.name());
            out.writeBoolean(task.global());
            out.writeString(task.key());

            // Write params
            out.writeInt(task.params().size());
            for (Map.Entry<String, Parameter> entry : task.params().entrySet()) {
                out.writeString(entry.getKey());
                new ParameterSerializer().write(out, entry.getValue());
            }

            out.writeString(task.command());

            // Write args
            out.writeInt(task.args().size());
            for (String arg : task.args()) {
                out.writeString(arg);
            }

            // Write env
            out.writeInt(task.env().size());
            for (Map.Entry<String, String> entry : task.env().entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }

            out.writeInt(task.timeout());
            out.writeInt(task.retry());

            // Write dependsOn
            out.writeInt(task.dependsOn().size());
            for (String dep : task.dependsOn()) {
                out.writeString(dep);
            }
        }

        private Task readTask(ObjectDataInput in) throws IOException {
            String name = in.readString();
            boolean global = in.readBoolean();
            String key = in.readString();

            // Read params
            int paramsSize = in.readInt();
            Map<String, Parameter> params = new HashMap<>(paramsSize);
            for (int i = 0; i < paramsSize; i++) {
                String paramName = in.readString();
                Parameter param = new ParameterSerializer().read(in);
                params.put(paramName, param);
            }

            String command = in.readString();

            // Read args
            int argsSize = in.readInt();
            List<String> args = new ArrayList<>(argsSize);
            for (int i = 0; i < argsSize; i++) {
                args.add(in.readString());
            }

            // Read env
            int envSize = in.readInt();
            Map<String, String> env = new HashMap<>(envSize);
            for (int i = 0; i < envSize; i++) {
                env.put(in.readString(), in.readString());
            }

            int timeout = in.readInt();
            int retry = in.readInt();

            // Read dependsOn
            int depsSize = in.readInt();
            List<String> dependsOn = new ArrayList<>(depsSize);
            for (int i = 0; i < depsSize; i++) {
                dependsOn.add(in.readString());
            }

            return new Task(name, global, key, params, command, args, env, timeout, retry, dependsOn);
        }

        private void writeObjectMap(ObjectDataOutput out, Map<String, Object> map) throws IOException {
            out.writeInt(map.size());
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                out.writeString(entry.getKey());
                writeObject(out, entry.getValue());
            }
        }

        private Map<String, Object> readObjectMap(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            Map<String, Object> map = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                Object value = readObject(in);
                map.put(key, value);
            }
            return map;
        }

        private void writeObject(ObjectDataOutput out, Object obj) throws IOException {
            if (obj == null) {
                out.writeByte(0);
            } else if (obj instanceof String) {
                out.writeByte(1);
                out.writeString((String) obj);
            } else if (obj instanceof Integer) {
                out.writeByte(2);
                out.writeInt((Integer) obj);
            } else if (obj instanceof Boolean) {
                out.writeByte(3);
                out.writeBoolean((Boolean) obj);
            } else if (obj instanceof Long) {
                out.writeByte(4);
                out.writeLong((Long) obj);
            } else if (obj instanceof Double) {
                out.writeByte(5);
                out.writeDouble((Double) obj);
            } else {
                out.writeByte(1);
                out.writeString(obj.toString());
            }
        }

        private Object readObject(ObjectDataInput in) throws IOException {
            byte type = in.readByte();
            return switch (type) {
                case 0 -> null;
                case 1 -> in.readString();
                case 2 -> in.readInt();
                case 3 -> in.readBoolean();
                case 4 -> in.readLong();
                case 5 -> in.readDouble();
                default -> throw new IOException("Unknown object type: " + type);
            };
        }
    }
}