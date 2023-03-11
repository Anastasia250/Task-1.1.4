package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    Util util = new Util();
    User user = new User();

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        PreparedStatement preparedStatement;

        try {
            preparedStatement = util.getConnection().prepareStatement("CREATE SEQUENCE IF NOT EXISTS hibernate_sequence START WITH 1 INCREMENT BY 1;\n" +
                    "\n" +
                    "CREATE TABLE users\n" +
                    "(\n" +
                    "    id       BIGINT NOT NULL,\n" +
                    "    name     VARCHAR(255),\n" +
                    "    lastName VARCHAR(255),\n" +
                    "    age      SMALLINT,\n" +
                    "    CONSTRAINT pk_users PRIMARY KEY (id)\n" +
                    ");");

            preparedStatement.executeUpdate();
        } catch (SQLException throwables) {
            dropUsersTable();
            createUsersTable();
        }
    }

    public void dropUsersTable() {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = util.getConnection().prepareStatement("drop table users");
            preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            createUsersTable();
            dropUsersTable();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = util.getConnection().prepareStatement("INSERT INTO users (id, name, lastName, age) VALUES (nextval('hibernate_sequence'),?, ?, ?);");
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            System.out.println("User с именем - " + name + " добавлен в базу данныых");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = util.getConnection().prepareStatement("delete from users where id = ?");
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> list = new LinkedList<>();

        PreparedStatement preparedStatement;
        try {
            preparedStatement = util.getConnection().prepareStatement("select * from users");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                user.setId(resultSet.getLong(1));
                user.setName(resultSet.getString(2));
                user.setLastName(resultSet.getString(3));
                user.setAge(resultSet.getByte(4));
                list.add(user);

                System.out.println(user);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return list;
    }

    public void cleanUsersTable() {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = util.getConnection().prepareStatement("Truncate table users");
            preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
