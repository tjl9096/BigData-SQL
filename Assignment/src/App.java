/*
 * Assignment 2
 * Tyler Lapiana - tjl9096
 * 
 * This project will cover the code portions of assignment 2.
 * 
 * 
 * ********************************************************************
 * When I ran the code, I used Visual Studio Code's "Run Java" button.
 * This was after making a new java project, having the code in the
 * src folder and adding the "postgresql-42.7.1.jar" file to the lib
 * folder.
 * 
 * To run the project, you should only have to modify a few things in the 
 * actual code. The first is the url, username, and password for 
 * postgres. This can be found in the main function. The second is in
 * the populateTables function where you will have to change the 
 * path to all of the data files. 
 * 
 * If you are curious about the specific requirements for each
 * function, the function header code block will have that information.
 * ********************************************************************
 */

import java.io.FileInputStream;
import java.sql.*;
import java.util.HashMap;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

public class App {

    /*
     * The main function of the program does some managerial tasks, sets up the connection
     * to the database, and calls all of the functions that implement the assignment questions
     * 
     * If you only want to run certain sections of the assignment at any point, just comment out
     * the function calls to the sections you don't want. To know what each section does, read the
     * function header of that function.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("jdk.xml.totalEntitySizeLimit", "2147480000"); // increase read size to allow full files to be read
        System.setProperty("jdk.xml.entityExpansionLimit", "2147480000");

        Connection con = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        String url = "jdbc:postgresql://localhost/assignment2";     // modify these to match your requirements 
        String user = "postgres";
        String pwd = "postgrespassword";
        con = DriverManager.getConnection(url, user, pwd);
        con.setAutoCommit(true);

        st = con.prepareStatement(
            "SET client_encoding TO 'UTF8'"
        );
        st.executeUpdate();


        // Comment out any function call(s) that you dont want to currently use below

        makeInitialTables(con, st);
        populateTables(con, st);
        cleanTables(con, st);
        sqlQueries(con, st, rs);
        createIndexes(con, st);
        sqlQueries(con, st, rs);    // a second call to check execution time after indexing

        try {
            if (con != null)
                con.close();
            if (st != null)
                st.close();
            if (rs != null)
                rs.close();
        }
        catch (SQLException oops) {
            System.out.println("Something went really wrong, idk what happened");
        }

    }

    /*
     * This function should be used when the database already exists, but no tables are in it. It makes all the intial
     * tables for the assignment, but without the foreign key assignments. This is in order
     * to populate the data nicely. You can call cleanTables to remove extra data and add
     * the foreign key requirements.
     */
    public static void makeInitialTables(Connection con, PreparedStatement st) {
        try {
            // Users - in the database as "Yousers" because postgres doesn't like it for some reason
            st = con.prepareStatement(
                "CREATE TABLE Yousers ( " +
                "Id INTEGER, AccountId INTEGER, " +
                "DisplayName VARCHAR(100), " +
                "AboutMe TEXT, CreationDate TIMESTAMP, " +
                "Reputation INTEGER, " +
                "PRIMARY KEY (Id) )"
            );
            st.execute();

            // Posts
            st = con.prepareStatement(
                "CREATE TABLE Posts ( " +
                "Id INTEGER, ParentId INTEGER, OwnerUserId INTEGER, " +
                "AcceptedAnswerId INTEGER, Title VARCHAR(300), " +
                "Body TEXT, Score INTEGER, " +
                "ViewCount INTEGER, CreationDate TIMESTAMP, " +
                "PRIMARY KEY (Id) )"
            );
            st.execute();

            // Tags
            st = con.prepareStatement(
                "CREATE TABLE Tags ( " +
                "Id INTEGER, TagName VARCHAR(100), " +
                "PRIMARY KEY (Id) )"
            );
            st.execute();

            // PostTags
            st = con.prepareStatement(
                "CREATE TABLE PostTags ( " +
                "PostId INTEGER, TagId INTEGER, " +
                "PRIMARY KEY (PostId, TagId) )"
            );
            st.execute();

            // Badges
            st = con.prepareStatement(
                "CREATE TABLE Badges ( " +
                "Id INTEGER, UserId INTEGER, " +
                "Name VARCHAR(100), Date TIMESTAMP, " +
                "PRIMARY KEY (Id) )"
            );
            st.execute();

            // Comments
            st = con.prepareStatement(
                "CREATE TABLE Comments ( " +
                "Id INTEGER, PostId INTEGER, " +
                "Score INTEGER, Text TEXT, " +
                "CreationDate TIMESTAMP, UserId INTEGER, " +
                "PRIMARY KEY (Id) )"
            );
            st.execute();
        }
        catch (SQLException oops) {
            System.out.println("Making the tables went wrong, printing stack trace:");
            oops.printStackTrace();
        }
    }

    /*
     * This function reads the data files and inserts them into the database without doing any of the cleaning.
     * 
     * For this function to work properly, the database and tables must be made, but nothing should be in them.
     */
    public static void populateTables(Connection con, PreparedStatement st) {
        System.out.println("Filling Users");
        try (FileInputStream usersInputStream = new FileInputStream("C:\\Users\\Tyler\\CSCI620\\Assignment2\\askubuntu\\Users.xml")) {      // change me
            XMLInputFactory usersXMLFactory = XMLInputFactory.newInstance();
            XMLStreamReader usersReader = usersXMLFactory.createXMLStreamReader(usersInputStream);
            while (usersReader.hasNext()) {
                int eventType = usersReader.next();
                if (eventType == XMLStreamReader.START_ELEMENT) {
                    String elementName = usersReader.getLocalName();
                    if (elementName.equals("row")) {
                        st = con.prepareStatement(
                            "INSERT INTO Yousers(" +
                            "Id, AccountId, DisplayName, AboutMe, CreationDate, Reputation) " +
                            "VALUES (?, ?, ?, ?, ?, ?)"
                        );
                        st.setNull(1, java.sql.Types.INTEGER);
                        st.setNull(2, java.sql.Types.INTEGER);
                        st.setNull(3, java.sql.Types.VARCHAR);
                        st.setNull(4, java.sql.Types.VARCHAR);
                        st.setNull(5, java.sql.Types.TIMESTAMP);
                        st.setNull(6, java.sql.Types.INTEGER);

                        for (int i = 0; i < usersReader.getAttributeCount(); i++) {
                            String attrName = usersReader.getAttributeLocalName(i);
                            switch (attrName) {
                                case "Id":
                                    st.setInt(1, Integer.valueOf(usersReader.getAttributeValue(i)));
                                    break;
                                case "AccountId":
                                    st.setInt(2, Integer.valueOf(usersReader.getAttributeValue(i)));
                                    break;
                                case "DisplayName":
                                    st.setString(3, usersReader.getAttributeValue(i));
                                    break;
                                case "AboutMe":
                                    st.setString(4, usersReader.getAttributeValue(i));
                                    break;
                                case "CreationDate":
                                    st.setTimestamp(5, Timestamp.valueOf(usersReader.getAttributeValue(i).replace("T", " ")));
                                    break;
                                case "Reputation":
                                    st.setInt(6, Integer.valueOf(usersReader.getAttributeValue(i)));
                                    break;
                            }
                        }

                        st.executeUpdate();
                    }
                }
            }
        }
        catch (Exception oops) {
            System.out.println("Error when parsing Users xml file and entering data");
            oops.printStackTrace();
        }

        HashMap<String, Integer> tagMap = new HashMap<>();

        System.out.println("Filling Tags");
        try (FileInputStream tagsInputStream = new FileInputStream("C:\\Users\\Tyler\\CSCI620\\Assignment2\\askubuntu\\Tags.xml")) {        // change me
            XMLInputFactory tagsXMLFactory = XMLInputFactory.newInstance();
            XMLStreamReader tagsReader = tagsXMLFactory.createXMLStreamReader(tagsInputStream);
            while (tagsReader.hasNext()) {
                int eventType = tagsReader.next();
                if (eventType == XMLStreamReader.START_ELEMENT) {
                    String elementName = tagsReader.getLocalName();
                    if (elementName.equals("row")) {
                        st = con.prepareStatement(
                            "INSERT INTO Tags(" +
                            "Id, TagName) " +
                            "VALUES(?, ?)" 
                        );
                        st.setNull(1, java.sql.Types.INTEGER);
                        st.setNull(2, java.sql.Types.VARCHAR);

                        Integer tempTagId = null;
                        String tempTagName = null;

                        for (int i = 0; i < tagsReader.getAttributeCount(); i++) {
                            String attrName = tagsReader.getAttributeLocalName(i);
                            switch (attrName) {
                                case "Id":
                                    st.setInt(1, Integer.valueOf(tagsReader.getAttributeValue(i)));
                                    tempTagId = Integer.valueOf(tagsReader.getAttributeValue(i));
                                    break;
                                case "TagName":
                                    st.setString(2, tagsReader.getAttributeValue(i));
                                    tempTagName = tagsReader.getAttributeValue(i);
                                    tagMap.put(tempTagName, tempTagId);
                                    break;
                            }
                        }

                        st.executeUpdate();
                    }
                }
            }
        }
        catch (Exception oops) {
            System.out.println("Error when parsing Tags xml file and entering data");
            oops.printStackTrace();
        }

        System.out.println("Filling Posts and PostTags");
        try (FileInputStream postsInputStream = new FileInputStream("C:\\Users\\Tyler\\CSCI620\\Assignment2\\askubuntu\\Posts.xml")) {      // change me
            XMLInputFactory postsXMLFactory = XMLInputFactory.newInstance();
            XMLStreamReader postsReader = postsXMLFactory.createXMLStreamReader(postsInputStream);
            while (postsReader.hasNext()) {
                int eventType = postsReader.next();
                if (eventType == XMLStreamReader.START_ELEMENT) {
                    String elementName = postsReader.getLocalName();
                    if (elementName.equals("row")) {
                        st = con.prepareStatement(
                            "INSERT INTO Posts(" + 
                            "Id, ParentId, OwnerUserId, AcceptedAnswerId, Title, Body, Score, ViewCount, CreationDate) " +
                            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        );
                        st.setNull(1, java.sql.Types.INTEGER);
                        st.setNull(2, java.sql.Types.INTEGER);
                        st.setNull(3, java.sql.Types.INTEGER);
                        st.setNull(4, java.sql.Types.INTEGER);
                        st.setNull(5, java.sql.Types.VARCHAR);
                        st.setNull(6, java.sql.Types.VARCHAR);
                        st.setNull(7, java.sql.Types.INTEGER);
                        st.setNull(8, java.sql.Types.INTEGER);
                        st.setNull(9, java.sql.Types.TIMESTAMP);

                        Integer tempPostId = null;

                        for (int i = 0; i < postsReader.getAttributeCount(); i++) {
                            String attrName = postsReader.getAttributeLocalName(i);
                            switch (attrName) {
                                case "Id":
                                    st.setInt(1, Integer.valueOf(postsReader.getAttributeValue(i)));
                                    tempPostId = Integer.valueOf(postsReader.getAttributeValue(i));
                                    break;
                                case "ParentId":
                                    st.setInt(2, Integer.valueOf(postsReader.getAttributeValue(i)));
                                    break;
                                case "OwnerUserId":
                                    st.setInt(3, Integer.valueOf(postsReader.getAttributeValue(i)));
                                    break;
                                case "AcceptedAnswerId":
                                    st.setInt(4, Integer.valueOf(postsReader.getAttributeValue(i)));
                                    break;
                                case "Title":
                                    st.setString(5, postsReader.getAttributeValue(i));
                                    break;
                                case "Body":
                                    if (postsReader.getAttributeValue(i).length() > 1000) {
                                        st.setString(6, postsReader.getAttributeValue(i).substring(0, 999));
                                    }
                                    else {
                                        st.setString(6, postsReader.getAttributeValue(i));
                                    }
                                    break;
                                case "Score":
                                    st.setInt(7, Integer.valueOf(postsReader.getAttributeValue(i)));
                                    break;
                                case "ViewCount":
                                    st.setInt(8, Integer.valueOf(postsReader.getAttributeValue(i)));
                                    break;
                                case "CreationDate":
                                    st.setTimestamp(9, Timestamp.valueOf(postsReader.getAttributeValue(i).replace("T", " ")));
                                    break;
                                case "Tags":
                                    String postTags = postsReader.getAttributeValue(i);
                                    postTags = postTags.replace("<", "");
                                    postTags = postTags.replace(">", ",");

                                    PreparedStatement st2 = con.prepareStatement(
                                        "INSERT INTO PostTags(" +
                                        "PostId, TagId) " +
                                        "VALUES (?, ?)"
                                    );

                                    while (postTags != "") {
                                        int nextTag = postTags.indexOf(",");
                                        if (nextTag != -1) {
                                            String tagtoEnter = postTags.substring(0, nextTag);

                                            if (tagMap.containsKey(tagtoEnter)) {
                                                st2.setInt(1, tempPostId);
                                                st2.setInt(2, tagMap.get(tagtoEnter));
                                                st2.executeUpdate();
                                            }
                                        }
                                        postTags = postTags.substring(nextTag + 1, postTags.length());
                                    }
                                    break;
                            }
                        }

                        st.executeUpdate();
                    }
                }
            }
        }
        catch (Exception oops) {
            System.out.println("Error when parsing Posts xml file and enetering data");
            oops.printStackTrace();
        }

        System.out.println("Filling Badges");
        try (FileInputStream badgeInputStream = new FileInputStream("C:\\Users\\Tyler\\CSCI620\\Assignment2\\askubuntu\\Badges.xml")) {     // change me
            XMLInputFactory badgeXMLFactory = XMLInputFactory.newInstance();
            XMLStreamReader badgeReader = badgeXMLFactory.createXMLStreamReader(badgeInputStream);
            while (badgeReader.hasNext()) {
                int eventType = badgeReader.next();
                if (eventType == XMLStreamReader.START_ELEMENT) {
                    String elementName = badgeReader.getLocalName();
                    if (elementName.equals("row")) {
                        st = con.prepareStatement(
                            "INSERT INTO Badges(" +
                            "Id, UserId, Name, Date) " +
                            "VALUES(?, ?, ?, ?)"
                        );
                        st.setNull(1, java.sql.Types.INTEGER);
                        st.setNull(2, java.sql.Types.INTEGER);
                        st.setNull(3, java.sql.Types.VARCHAR);
                        st.setNull(4, java.sql.Types.TIMESTAMP);

                        for (int i = 0; i < badgeReader.getAttributeCount(); i++) {
                            String attrName = badgeReader.getAttributeLocalName(i);
                            switch (attrName) {
                                case "Id":
                                    st.setInt(1, Integer.valueOf(badgeReader.getAttributeValue(i)));
                                    break;
                                case "UserId":
                                    st.setInt(2, Integer.valueOf(badgeReader.getAttributeValue(i)));
                                    break;
                                case "Name":
                                    st.setString(3, badgeReader.getAttributeValue(i));
                                    break;
                                case "Date":
                                    st.setTimestamp(4, Timestamp.valueOf(badgeReader.getAttributeValue(i).replace("T", " ")));
                                    break;
                            }
                        }

                        st.executeUpdate();
                    }
                }
            }
        }
        catch (Exception oops) {
            System.out.println("Error when parsing Badge xml file and entering data");
            oops.printStackTrace();
        }

        System.out.println("Filling Comments");
        try (FileInputStream commentInputStream = new FileInputStream("C:\\Users\\Tyler\\CSCI620\\Assignment2\\askubuntu\\Comments.xml")) {     // change me
            XMLInputFactory commentXMLFactory = XMLInputFactory.newInstance();
            XMLStreamReader commentReader = commentXMLFactory.createXMLStreamReader(commentInputStream);
            while (commentReader.hasNext()) {
                int entityType = commentReader.next();
                if (entityType == XMLStreamReader.START_ELEMENT) {
                    String elementName = commentReader.getLocalName();
                    if (elementName.equals("row")) {
                        st = con.prepareStatement(
                            "INSERT INTO Comments(" +
                            "Id, PostId, Score, Text, CreationDate, UserId) " +
                            "VALUES(?, ?, ?, ?, ?, ?)"
                        );
                        st.setNull(1, java.sql.Types.INTEGER);
                        st.setNull(2, java.sql.Types.INTEGER);
                        st.setNull(3, java.sql.Types.INTEGER);
                        st.setNull(4, java.sql.Types.VARCHAR);
                        st.setNull(5, java.sql.Types.TIMESTAMP);
                        st.setNull(6, java.sql.Types.INTEGER);

                        for (int i = 0; i < commentReader.getAttributeCount(); i++) {
                            String attrName = commentReader.getAttributeLocalName(i);
                            switch (attrName) {
                                case "Id":
                                    st.setInt(1, Integer.valueOf(commentReader.getAttributeValue(i)));
                                    break;
                                case "PostId":
                                    st.setInt(2, Integer.valueOf(commentReader.getAttributeValue(i)));
                                    break;
                                case "Score":
                                    st.setInt(3, Integer.valueOf(commentReader.getAttributeValue(i)));
                                    break;
                                case "Text":
                                    st.setString(4, commentReader.getAttributeValue(i));
                                    break;
                                case "CreationDate":
                                    st.setTimestamp(5, Timestamp.valueOf(commentReader.getAttributeValue(i).replace("T", " ")));
                                    break;
                                case "UserId":
                                    st.setInt(6, Integer.valueOf(commentReader.getAttributeValue(i)));
                                    break;
                            }
                        }

                        st.executeUpdate();
                    }
                }
            }
        }
        catch (Exception oops) {
            System.out.println("Error when parsing Comments xml and entering data");
            oops.printStackTrace();
        }

        System.out.println("Done filling tables\n");
    }

    /*
     * This function goes through the database and removes any values that would not fulfil
     * the foreign key requirements, then inserts the foreign key requirements.
     * 
     * For this function to run properly, the database should be filled with the uncleaned data
     */
    public static void cleanTables(Connection con, PreparedStatement st) {
        try {
            // First, make sure that all foreign key requirements are met before putting the requirements in

            // Posts
            System.out.println("Cleaning Tables");

            // I'm looping the delete's here becasue of a weird error where running this section once
            // doesn't get everything. I am assuming this is because of the order they are being removed
            // so running it twice solves this
            for (int i = 0; i < 2; i++) {
                System.out.println("Posts - OwnerUserId FK");
                st = con.prepareStatement(
                    "DELETE FROM Posts " +
                    "WHERE OwnerUserId IN " +
                        "(SELECT OwnerUserId FROM Posts EXCEPT SELECT Id FROM Yousers)"
                );
                st.executeUpdate();

                System.out.println("Posts - ParentId FK");
                st = con.prepareStatement(
                    "DELETE FROM Posts " +
                    "WHERE ParentId IN " +
                        "(SELECT ParentId FROM Posts EXCEPT SELECT Id FROM Posts)"
                );
                st.executeUpdate();

                System.out.println("Posts - AcceptedAnswerId FK");
                st = con.prepareStatement(
                    "DELETE FROM Posts " +
                    "WHERE AcceptedAnswerId IN " +
                        "(SELECT AcceptedAnswerId FROM Posts EXCEPT SELECT Id FROM Posts)"
                );
                st.executeUpdate();

                // PostTags
                System.out.println("PostTags PostId FK");
                st = con.prepareStatement(
                    "DELETE FROM PostTags " +
                    "WHERE PostId IN " +
                        "(SELECT PostId FROM PostTags EXCEPT SELECT Id FROM Posts)"
                );
                st.executeUpdate();

                System.out.println("PostTags TagId FK");
                st = con.prepareStatement(
                    "DELETE FROM PostTags " +
                    "WHERE TagId IN " +
                        "(SELECT TagId FROM PostTags EXCEPT SELECT Id FROM Tags)"
                );
                st.executeUpdate();

                // Badges
                System.out.println("Badges UserId FK");
                st = con.prepareStatement(
                    "DELETE FROM Badges " +
                    "WHERE UserId IN " +
                        "(SELECT UserId FROM Badges EXCEPT SELECT Id FROM Yousers)"
                );
                st.executeUpdate();

                // Comments
                System.out.println("Comments PostId FK");
                st = con.prepareStatement(
                    "DELETE FROM Comments " +
                    "WHERE PostId IN " +
                        "(SELECT PostId FROM Comments EXCEPT SELECT Id FROM Posts)"
                );
                st.executeUpdate();

                System.out.println("Comments UserId FK");
                st = con.prepareStatement(
                    "DELETE FROM Comments " +
                    "WHERE UserId IN " +
                        "(SELECT UserId FROM Comments EXCEPT SELECT Id FROM Yousers)"
                );
                st.executeUpdate();
            }
            
            // Then, we can update the tables to have the foreign key constraints
            System.out.println("Altering Tables to include foreign keys");

            // Turn the checks off since above we guaranteed the constraints already
            st = con.prepareStatement(      
                "ALTER TABLE Posts DISABLE TRIGGER ALL"
            );
            st.executeUpdate();
            st = con.prepareStatement(
                "ALTER TABLE PostTags DISABLE TRIGGER ALL"
            );
            st.executeUpdate();
            st = con.prepareStatement(
                "ALTER TABLE Badges DISABLE TRIGGER ALL"
            );
            st.executeUpdate();
            st = con.prepareStatement(
                "ALTER TABLE Comments DISABLE TRIGGER ALL"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE Posts " +
                "ADD FOREIGN KEY (OwnerUserId) REFERENCES Yousers(Id)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE Posts " +
                "ADD FOREIGN KEY (ParentId) REFERENCES Posts(Id)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE Posts " +
                "ADD FOREIGN KEY (AcceptedAnswerId) REFERENCES Posts(Id)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE PostTags " +
                "ADD FOREIGN KEY (PostId) REFERENCES Posts(Id)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE PostTags " +
                "ADD FOREIGN KEY (TagId) REFERENCES Tags(Id)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE Badges " +
                "ADD FOREIGN KEY (UserId) REFERENCES Yousers(Id)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE Comments " +
                "ADD FOREIGN KEY (PostId) REFERENCES Posts(Id)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "ALTER TABLE Comments " +
                "ADD FOREIGN KEY (UserId) REFERENCES Yousers(Id)"
            );
            st.executeUpdate();
            
            // turn the checks back on
            st = con.prepareStatement(      
                "ALTER TABLE Posts ENABLE TRIGGER ALL"
            );
            st.executeUpdate();
            st = con.prepareStatement(
                "ALTER TABLE PostTags ENABLE TRIGGER ALL"
            );
            st.executeUpdate();
            st = con.prepareStatement(
                "ALTER TABLE Badges ENABLE TRIGGER ALL"
            );
            st.executeUpdate();
            st = con.prepareStatement(
                "ALTER TABLE Comments ENABLE TRIGGER ALL"
            );
            st.executeUpdate();
        }
        catch (SQLException oops) {
            System.out.println("Cleaning the tables/data went wrong, printing stack trace:");
            oops.printStackTrace();
        }

        System.out.println("Done cleaning tables");
    }

    /*
     * This function runs the queries that are asked in question 2 of the assignment.
     * 
     * For this function to work properly, the database needs to be filled with the data 
     * and it had to have been cleaned already
     */
    static void sqlQueries(Connection con, PreparedStatement st, ResultSet rs) {
        // 2.1
        try {
            st = con.prepareStatement(
                "SELECT Name FROM Badges as b " +
                "JOIN Yousers as u ON b.UserId = u.Id " +
                "WHERE b.Date BETWEEN u.CreationDate AND u.CreationDate + interval '1 year' " +
                "GROUP BY b.Name " +
                "ORDER BY COUNT(b.Name) DESC " +
                "LIMIT 10"
            );
            long start = System.currentTimeMillis();
            rs = st.executeQuery();
            long end = System.currentTimeMillis();
            double elapsed = (end - start) / 1000.0;

            System.out.println("Query 2.1 took " + elapsed + " seconds to run");
            System.out.println("The result was the following:\n");

            while(rs.next()) {
                System.out.println(rs.getString("Name"));
            }
            System.out.println();

        }
        catch (Exception oops) {
            System.out.println("Error doing query 2.1, printing stack trace");
            oops.printStackTrace();
        }

        // 2.2
        try {
            st = con.prepareStatement(
                "SELECT DisplayName FROM Yousers " +
                "WHERE Reputation > 1000 AND Yousers.Id IN ( " +
                    "SELECT Id FROM Yousers " +
                    "EXCEPT " +
                    "SELECT OwnerUserId FROM Posts " +
                ")"
            );
            long start = System.currentTimeMillis();
            rs = st.executeQuery();
            long end = System.currentTimeMillis();
            double elapsed = (end - start) / 1000.0;

            System.out.println("Query 2.2 took " + elapsed + " seconds to run");
            System.out.println("The result was the following:\n");

            while(rs.next()) {
                System.out.println(rs.getString("DisplayName"));
            }
            System.out.println();

        }
        catch (Exception oops) {
            System.out.println("Error doing query 2.2, printing stack trace");
            oops.printStackTrace();
        }

        // 2.3
        try {
            st = con.prepareStatement(
                "SELECT u.DisplayName, u.Reputation FROM Yousers as u " +
                "JOIN Posts as p ON u.Id = p.OwnerUserId " +
                "JOIN PostTags as pt ON p.ParentId = pt.PostId " +
                "JOIN Tags as t ON pt.TagId = t.Id " +
                "WHERE t.TagName = 'postgresql' " +
                "GROUP BY u.DisplayName, u.Reputation " +
                "HAVING COUNT(u.DisplayName) > 1" 
            );
            long start = System.currentTimeMillis();
            rs = st.executeQuery();
            long end = System.currentTimeMillis();
            double elapsed = (end - start) / 1000.0;

            System.out.println("Query 2.3 took " + elapsed + " seconds to run");
            System.out.println("The result was the following:\n");

            while(rs.next()) {
                System.out.println(rs.getString("DisplayName") + " - " + rs.getString("Reputation"));
            }
            System.out.println();

        }
        catch (Exception oops) {
            System.out.println("Error doing query 2.3, printing stack trace");
            oops.printStackTrace();
        }

        // 2.4
        try {
            st = con.prepareStatement(
                "SELECT u.DisplayName FROM Yousers as u " +
                "JOIN Comments as c on u.Id = c.UserId " +
                "WHERE c.Score > 10 AND " +
                "c.CreationDate BETWEEN u.CreationDate AND u.CreationDate + interval '1 week' " +
                "GROUP BY u.DisplayName"
            );
            long start = System.currentTimeMillis();
            rs = st.executeQuery();
            long end = System.currentTimeMillis();
            double elapsed = (end - start) / 1000.0;

            System.out.println("Query 2.4 took " + elapsed + " seconds to run");
            System.out.println("The result was the following: (only the first 100 rows)\n");

            for(int i = 0; i < 100; i++) {
                if(!rs.next()) {
                    break;
                }
                System.out.println(rs.getString("DisplayName"));
            }
            System.out.println();

        }
        catch (Exception oops) {
            System.out.println("Error doing query 2.4, printing stack trace");
            oops.printStackTrace();
        }

        // 2.5
        try {
            st = con.prepareStatement(
                "SELECT t.TagName, COUNT(t.TagName) as TagCount FROM Tags as t " +
                "JOIN PostTags as pt ON t.Id = pt.TagId " +
                "JOIN Posts as p ON pt.PostId = p.id " +
                "JOIN PostTags as pt_two ON p.Id = pt_two.PostId " +
                "JOIN Tags as t_two ON pt_two.TagId = t_two.Id " +
                "WHERE t_two.TagName = 'postgresql' AND t.TagName != 'postgresql' " +
                "GROUP BY t.TagName " +
                "ORDER BY COUNT(t.TagName) DESC, t.TagName ASC " +
                "LIMIT 25"
            );
            long start = System.currentTimeMillis();
            rs = st.executeQuery();
            long end = System.currentTimeMillis();
            double elapsed = (end - start) / 1000.0;

            System.out.println("Query 2.5 took " + elapsed + " seconds to run");
            System.out.println("The result was the following:\n");

            while(rs.next()) {
                System.out.println(rs.getString("TagName") + " - " + rs.getString("TagCount"));
            }
            System.out.println();

        }
        catch (Exception oops) {
            System.out.println("Error doing query 2.5, printing stack trace");
            oops.printStackTrace();
        }
    }

    /*
     * This function inserts the indexes asked in question 5 of the assignment.
     * To check the indexes, run the sqlQueries method again
     * 
     * For this function to work properly, the database needs to be filled with the data 
     * and it had to have been cleaned already
     */
    static void createIndexes(Connection con, PreparedStatement st) {
        System.out.println("Creating Indexes");
        try {
            st = con.prepareStatement(
                "CREATE INDEX badges_UserId ON Badges(UserId)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX badges_Date ON Badges(Date)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX yousers_CreationDate ON Yousers(CreationDate)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX yousers_Reputation ON Yousers(Reputation)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX yousers_DisplayName ON Yousers(DisplayName)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX posts_OwnerUserId ON Posts(OwnerUserId)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX posts_ParentId ON Posts(ParentId)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX tags_TagName ON Tags(TagName)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX posttags_TagId ON PostTags(TagId)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX comments_UserId ON Comments(UserId)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX comments_Score ON Comments(Score)"
            );
            st.executeUpdate();

            st = con.prepareStatement(
                "CREATE INDEX comments_CreationDate ON Comments(CreationDate)"
            );
            st.executeUpdate();
        }
        catch (Exception oops){
            System.out.println("Error creating indexes, printing stack strace");
            oops.printStackTrace();
        }
    }
}
