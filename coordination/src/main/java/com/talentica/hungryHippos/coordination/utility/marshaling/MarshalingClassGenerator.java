package com.talentica.hungryHippos.coordination.utility.marshaling;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.*;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by debasishc on 1/9/15.
 * Will see to it later
 */
public class MarshalingClassGenerator {

    private static final Map<Class,Integer> sizeMap = new HashMap<>();

    static {

        sizeMap.put(byte.class,1);
        sizeMap.put(short.class,2);
        sizeMap.put(int.class,4);
        sizeMap.put(long.class,8);
        sizeMap.put(float.class,4);
        sizeMap.put(double.class,8);
        //At this point, store only upto 50 bytes in string
        sizeMap.put(String.class,50);

    }

    public static <E> Class<? extends E> generateMarshalingImplementation(
            Class<E> interfaceClass, String generatedClassName) {
        List<Method> getterMethods = new LinkedList<>();
        List<Method> setterMethods = new LinkedList<>();
        int buffSize = 0;
        for (Method m : interfaceClass.getMethods()) {
            if (m.getName().startsWith("get")) {
                getterMethods.add(m);
                Class<?> returnType = m.getReturnType();
                int s = sizeMap.get(returnType);
                buffSize+=s;
            } else if (m.getName().startsWith("set")) {
                setterMethods.add(m);
            }
        }

        ClassNode classNode = new ClassNode();
        generatedClassName = generatedClassName.replaceAll("\\.", "/");
        // These properties of the classNode must be set
        classNode.version = Opcodes.V1_8;// The generated class will only run on
        // JRE 1.6 or above
        classNode.access = Opcodes.ACC_PUBLIC;
        classNode.signature = "L" + generatedClassName + ";";
        classNode.name = generatedClassName;
        classNode.superName = "java/lang/Object";
        classNode.interfaces.add(Type.getInternalName(interfaceClass));

        FieldNode byteBufferNode = new FieldNode(Opcodes.V1_8,Opcodes.ACC_PUBLIC,
                "byteBuffer", Type.getInternalName(ByteBuffer.class),
                Type.getDescriptor(ByteBuffer.class), null);
        classNode.fields.add(byteBufferNode);


        //create the constructor
        MethodNode constructor = new MethodNode(
                Opcodes.ACC_PUBLIC,
                "<init>",
                "()V",
                null, null);

        constructor.instructions.add(
                new TypeInsnNode(Opcodes.NEW, Type.getInternalName(ByteBuffer.class)));
        constructor.instructions.add(new InsnNode(Opcodes.DUP));
        constructor.instructions.add(
                new MethodInsnNode(Opcodes.INVOKESPECIAL,
                        Type.getInternalName(ByteBuffer.class), "<init>", "()V", false));
        constructor.instructions.add(new InsnNode(Opcodes.RETURN));

        classNode.methods.add(constructor);

        int offset=0;
        for(Method m:getterMethods){
            MethodNode method = new MethodNode(
                    Opcodes.ACC_PUBLIC,
                    m.getName(),
                    "()"+Type.getDescriptor(m.getReturnType()),
                    null, null);
            int size = sizeMap.get(m.getReturnType());
            for(int i=0;i<size;i++){
                method.instructions.add(
                        new FieldInsnNode(Opcodes.GETFIELD, generatedClassName,"byteBuffer",
                                Type.getDescriptor(ByteBuffer.class)));

                switch (Type.getDescriptor(m.getReturnType())){
                    case "B":

                    case "S":
                    case "I":
                        method.instructions.add(new LdcInsnNode(0));
                        break;
                    case "J":
                        method.instructions.add(new LdcInsnNode(0L));


                }

            }
        }
        return null;
    }
}
