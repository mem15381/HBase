package org.example;




// Java program to convert String to Date
// Using DateTimeFormatter Class

// Importing required classes
import java.time.LocalDate;
        import java.time.format.DateTimeFormatter;
        import java.time.format.DateTimeParseException;

// Main class
class shamsi {

    // Method 1
    // To convert String to Date
    public static LocalDate
    getDateFromString(String string,
                      DateTimeFormatter format)
    {
        // Converting the string to date
        // in the specified format
        LocalDate date = LocalDate.parse(string, format);

        // Returning the converted date
        return date;
    }

    // Method 2
    // Main driver method
    public static void main(String[] args)
    {
        // Getting the custom string input
        String string = "28 October, 2018";

        // Getting the format by creating an object of
        // DateTImeFormatter class
        DateTimeFormatter format
                = DateTimeFormatter.ofPattern("d MMMM, yyyy");

        // Try block to check for exceptions
        try {

            // Getting the Date from String
            LocalDate date
                    = getDateFromString(string, format);

            // Printing the converted date
            System.out.println(date);
        }

        // Block 1
        // Catch block to handle exceptions occurring
        // if the String pattern is invalid
        catch (IllegalArgumentException e) {

            // Display the exception
            System.out.println("Exception: " + e);
        }

        // Block 2
        // If the String was unable to be parsed
        catch (DateTimeParseException e) {

            // Display the exception
            System.out.println("Exception: " + e);
        }
    }
}