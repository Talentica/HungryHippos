package com.talentica.hungryhippos.filesystem.main;

import java.util.Scanner;

import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.NodeFileSystem;

public class HungryHipposFileSystemMain {

	enum Operations {
		LS(0), TOUCH(1), MKDIR(2), FIND(3), DELETE(4), DELETEALL(5), EXIT(6);

		private int option = 0;

		Operations(int option) {
			this.option = option;
		}

		public int getOption() {
			return this.option;
		}

		public static Operations getOpertionsFromOption(int option) {
			return Operations.values()[option];
		}

	}

	private static String[] commands = { "ls", "touch", "mkdir", "find", "delete", "deleteall", "exit" };
	private static HungryHipposFileSystem hhfs = null;

	public static void getHHFSInstance() {
		hhfs = HungryHipposFileSystem.getInstance();
	}

	public static void main(String[] args) {

		if (args == null || args.length == 0) {
			System.out.println("Please choose what operation you want to do");
			System.out.println("ls \"fileName\"");
			System.out.println("touch \"fileName\"");
			System.out.println("mkdir \"dirName\"");
			System.out.println("find \"fileName\" ");
			System.out.println("delete \"fileName\"");
			System.out.println("deleteall \"fileName\"");
			System.out.println("exit");
			Scanner sc = new Scanner(System.in);
			if (sc.hasNext()) {
				String s = sc.nextLine();
				if (s != null) {
					String[] operationFileName = s.split(" ");
					getCommandDetails(operationFileName[0], operationFileName[1]);
				}
			}
			sc.close();
		} else {
			if (args.length == 2) {
				getCommandDetails(args[0], args[1]);
			} else {
				getCommandDetails(args[0], null);
			}
		}

	}

	public static void getCommandDetails(String operation, String name) {

		if (hhfs == null) {
			throw new RuntimeException(
					"HungryHipposFileSystem is not created, please create it calling getHHFSInstance() method");
		}

		if (name == null) {
			name = "HungryHipposFs";
		}
		for (int i = 0; i < commands.length; i++) {
			if (operation.equalsIgnoreCase(commands[i])) {
				Operations op = Operations.getOpertionsFromOption(i);
				runOperation(op, name);
				break;
			}
		}

	}

	private static void runOperation(Operations op, String name) {
		switch (op) {
		case LS:
			hhfs.getChildZnodes(name);
			break;
		case TOUCH:
			hhfs.createZnode(name);
			break;
		case MKDIR:
			hhfs.createZnode(name);
			break;
		case FIND:
			hhfs.findZnodePath(name);
			break;
		case DELETE:
			hhfs.deleteNode(name);
			break;
		case DELETEALL:
			hhfs.deleteNodeRecursive(name);
			break;
		default:
		}

	}

}
