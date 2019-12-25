package mflix.api.daos;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.BasicBSONObject;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.parameters.P;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

  private final MongoCollection<User> usersCollection;
  //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
  //returns a Session object
  private final MongoCollection<Session> sessionsCollection;

  private final Logger log;

  @Autowired
  public UserDao(
      MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
    super(mongoClient, databaseName);
    CodecRegistry pojoCodecRegistry =
        fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
    log = LoggerFactory.getLogger(this.getClass());
    //TODO> Ticket: User Management - implement the necessary changes so that the sessions
    // collection returns a Session objects instead of Document objects.
    sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
  }

  /**
   * Inserts the `user` object in the `users` collection.
   *
   * @param user - User object to be added
   * @return True if successful, throw IncorrectDaoOperation otherwise
   */
  public boolean addUser(User user) {
    //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
    try {
      usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
    } catch (MongoException e) {
      if(ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY) {
        throw new IncorrectDaoOperation(" User is already the database");
      }
      return false;
    }
    return true;
    //TODO > Ticket: Handling Errors - make sure to only add new users
    // and not users that already exist.

  }

  /**
   * Creates session using userId and jwt token.
   *
   * @param userId - user string identifier
   * @param jwt - jwt string token
   * @return true if successful
   */
  public boolean createUserSession(String userId, String jwt) {
    //TODO> Ticket: User Management - implement the method that allows session information to be
    // stored in it's designated collection.
    Session session = new Session();
    session.setUserId(userId);
    session.setJwt(jwt);
    //TODO > Ticket: Handling Errors - implement a safeguard against
    // creating a session with the same jwt token.
    Bson jwtFilter = Filters.in("jwt", jwt);
    Bson userIdFilter = Filters.in("user_id", userId);
    System.out.println("JWT: " + jwt);
    try {
      if (Optional.ofNullable(sessionsCollection.find(userIdFilter).first()).isPresent()) {
        sessionsCollection.updateOne(userIdFilter, new Document("$set", new Document("jwt", jwt)));
      } else {
        sessionsCollection.insertOne(session);
      }
    } catch (MongoException e) {
      return false;
    }

    return true;
  }

  /**
   * Returns the User object matching the an email string value.
   *
   * @param email - email string to be matched.
   * @return User object or null.
   */
  public User getUser(String email) {
    Bson emailFilter = Filters.in("email", email);
    User user = usersCollection.find(emailFilter).first();
    //TODO> Ticket: User Management - implement the query that returns the first User object.
    return user;
  }

  /**
   * Given the userId, returns a Session object.
   *
   * @param userId - user string identifier.
   * @return Session object or null.
   */
  public Session getUserSession(String userId) {
    //TODO> Ticket: User Management - implement the method that returns Sessions for a given
    // userId
    Bson userIdFilter = Filters.in("user_id", userId);
    Session session = sessionsCollection.find(userIdFilter).first();
    return session;
  }

  public boolean deleteUserSessions(String userId) {
    //TODO> Ticket: User Management - implement the delete user sessions method
    Bson userIdFilter = Filters.in("user_id", userId);
    boolean deleted = sessionsCollection.deleteOne(userIdFilter).wasAcknowledged();
    return deleted;
  }

  /**
   * Removes the user document that match the provided email.
   *
   * @param email - of the user to be deleted.
   * @return true if user successfully removed
   */
  public boolean deleteUser(String email) {
    // remove user sessions
    //TODO> Ticket: User Management - implement the delete user method
    //TODO > Ticket: Handling Errors - make this method more robust by
    // handling potential exceptions.
    Bson emailFilter = Filters.in("email", email);
    Bson userIdFilter = Filters.in("user_id", email);
    boolean sessionDeleted = sessionsCollection.deleteOne(userIdFilter).wasAcknowledged();
    if (!sessionDeleted) {
      return sessionDeleted;
    }
    boolean userdeleted = usersCollection.deleteOne(emailFilter).wasAcknowledged();
    return userdeleted;
  }

  /**
   * Updates the preferences of an user identified by `email` parameter.
   *
   * @param email - user to be updated email
   * @param userPreferences - set of preferences that should be stored and replace the existing
   *     ones. Cannot be set to null value
   * @return User object that just been updated.
   */
  public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
    //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
    // be updated.
    //TODO > Ticket: Handling Errors - make this method more robust by
    // handling potential exceptions when updating an entry.
    if (Objects.isNull(userPreferences)) {
        throw new IncorrectDaoOperation("User Preferences is Null");
    }
    Bson emailFilter = Filters.in("email", email);
    Document updDoc = new Document("preferences", userPreferences);
    boolean updated = usersCollection.updateOne(emailFilter, new Document("$set", updDoc)).wasAcknowledged();
    return updated;
  }
}
