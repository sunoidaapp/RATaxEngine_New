package vision;

public class TAXEngineMain {

	public static void main(String[] args) {
		TAXEngineProcess taxEngineMain = new TAXEngineProcess();

		if (args.length != 6) {
			System.out.println(
					"Invalid input parameters. Correct syntax is (6 arguments) <country> <le_book> <business_date DD-MON-YYYY> <business_line_id> <debug_mode (Y/N)> <log_path_with_file_name>.");
			System.exit(1); // Eclipse
		}

		int idx = 0;
		for (String s : args) {
			System.out.println((idx + 1) + ". Argument Value:" + s);
			idx++;
		}

		int returnValue;

		// returnValue = taxEngineMain.doTaxActivityProcess("KE", "01", "12-AUG-2021",
		// "BLBRNS0060", "Y");
		returnValue = taxEngineMain.doTaxActivityProcess(args[0], args[1], args[2], args[3], args[4], args[5]); // Eclipse

		if (returnValue == 0) {
			System.out.println("Process Completed...");
		} else {
			System.out.println("Process Aborted...");
		}

		System.exit(returnValue);
	}

}