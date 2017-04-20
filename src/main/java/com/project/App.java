package com.project;

import org.apache.hadoop.util.ProgramDriver;


public class App {
	public static void main(String[] args) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("driver", Driver.class, "The driver program that runs multiple jobs");
			exitCode = pgd.run(args);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}


