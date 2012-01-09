<?php

/**
 * db.php
 *
 * Copyright (c) 2010-2012 Brad Proctor. (http://bradleyproctor.com)
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @author      Brad Proctor
 * @copyright   Copyright (c) 2010-2012 Brad Proctor
 * @license     MIT License (http://www.opensource.org/licenses/mit-license.php)
 * @link        http://bradleyproctor.com/
 * @version     1.1
 */
class Database_Exception extends \FuelException {}

class DB {

	private $read_conn = null;   // The read connection
	private $write_conn = null;  // The write connection
	private $last_result;		 // The last query result
	private $last_error;         // The last error
	private $sql;                // Last query

	/**
	 * Create the DB object
	 */

	private function __construct() {
		\Config::load('db', true);
	}

	/**
	 * Get an instance of the DB class
	 */
	public static function instance() {
		static $instance = null;

		if (!($instance instanceof DB)) {
			$instance = new DB();
		}
		return $instance;
	}

	/**
	 * Connect to a database
	 *
	 * @param string $type
	 * 		Set to 'read' for a read connection, 'write' for a write connection
	 *
	 * @param string $host
	 * 		Hostname to connect to
	 *
	 * @param string $user
	 * 		The database usename
	 *
	 * @param string $pass
	 * 		The database password
	 *
	 * @param string $name
	 * 		The database name
	 *
	 * @param int $port
	 * 		The port to use to connect
	 *
	 * @param string $char
	 * 		The character set
	 *
	 * @return bool
	 * 		Returns TRUE on success, false on error
	 */
	public function connect($type = 'read', $host = null, $user = null, $pass = null, $name = null, $port = null, $char = null) {
		try {
			$config = \Config::get('db.' . \Config::get('db.active'));
			$num_dbs = count($config);

			// Use the first connection, if this is a write connection, or there is only one server to use
			if ($type == 'write' || ($type == 'read' && $num_dbs == 1)) {
				$host or $host = $config[0]['hostname'];
				$user or $user = $config[0]['username'];
				$pass or $pass = $config[0]['password'];
				$name or $name = $config[0]['database'];
				$port or $port = $config[0]['port'];
				$char or $char = $config[0]['charset'];
			} else if ($type == 'read') {
				// Choose a random read server
				$i = rand(1, $num_dbs - 1);
				$host or $host = $config[$i]['hostname'];
				$user or $user = $config[$i]['username'];
				$pass or $pass = $config[$i]['password'];
				$name or $name = $config[$i]['database'];
				$port or $port = $config[$i]['port'];
				$char or $char = $config[$i]['charset'];
			} else {
				throw new Database_Exception('Invalid connection type selected', 0);
			}

			$conn = new mysqli($host, $user, $pass, $name, $port);
			if ($conn->error) {
				throw new Database_Exception($conn->error, $conn->errno);
			}

			if ($conn->set_charset($char) === false) {
				throw new Database_Exception($conn->error, $conn->errno);
			}
		} catch (ErrorException $e) {
			throw new Database_Exception('No MySQLi Connection: ' . $e->getMessage(), 0);
		}

		// If there is only one database, set both read and write so we don't end up with two connections to the same server
		if ($num_dbs == 1) {
			$this->write_conn = $this->read_conn = $conn;
		} else {
			($type === 'read') ? $this->read_conn = $conn : $this->write_conn = $conn;
		}

		return $conn->error;
	}

	/**
	 * Destroys this object and closes the database connection
	 *
	 * @return bool
	 *    Returns the FALSE if the database failed to close, TRUE on success
	 */
	public function __destruct() {
		return $this->close();
	}

	/**
	 * Close the database connection
	 *
	 * @return bool
	 *    Returns the FALSE if the database failed to close, TRUE on success
	 */
	public function close($type = null) {
		if ($type == 'read') {
			if ($this->read_conn instanceof mysqli) {
				return $this->read_conn->close();
			}
		} else if ($type == 'write') {
			if ($this->write_conn instanceof mysqli) {
				return $this->write_conn->close();
			}
		} else {
			if ($this->read_conn instanceof mysqli) {
				return $this->read_conn->close();
			}
			if ($this->write_conn instanceof mysqli) {
				return $this->write_conn->close();
			}
		}
		return true;
	}

	/**
	 * Returns the last error
	 *
	 * @return string
	 *    Returns the last error, FALSE if no error
	 */
	public function error() {
		return $this->last_error;
	}

	/**
	 * Free the memory from the last results
	 *
	 * @return bool
	 * 		Returns TRUE if the result was successfully freed, FALSE on error
	 */
	public function free() {
		if ($this->last_result instanceof mysqli_result) {
			$this->last_result->free();
			return true;
		}
		return false;
	}

	/**
	 * Returns the last inserted ID
	 *
	 * @return int
	 *    Returns the last insert ID, or FALSE if no insert ID
	 */
	public function insert_id() {
		if ($this->write_conn instanceof mysqli) {
			return $this->write_conn->insert_id;
		}
		return false;
	}

	/**
	 * Retuns the number of rows from the last query
	 *
	 * @return int
	 *    Return the number of rows the last query, or FALSE
	 */
	public function num_rows() {
		if ($this->last_result instanceof mysqli_result) {
			return $this->last_result->num_rows;
		}
		return false;
	}

	/**
	 * Returns the number of affected rows from the last query
	 *
	 * @return int
	 * 		Return the number of affected rows from last query, or FALSE
	 */
	public function affected_rows() {
		if ($this->last_result instanceof mysqli_result) {
			return $this->last_result->affected_rows;
		}
		return false;
	}

	/**
	 * Returns the last sql executed
	 *
	 * @return string
	 *    Returns the last sql executed
	 */
	public function last_query() {
		return $this->sql;
	}

	/**
	 * Begin a new transaction
	 */
	public function begin() {
		if (!($this->write_conn instanceof mysqli)) {
			$this->connect('write');
		}
		$this->write_conn->autocommit(false);
	}

	/**
	 * Commit the current transaction
	 */
	public function commit() {
		if (!($this->write_conn instanceof mysqli)) {
			$this->connect('write');
		}

		$this->write_conn->commit();
		$this->write_conn->autocommit(true);
	}

	/**
	 * Rollback a transaction
	 */
	public function rollback() {
		if (!($this->write_conn instanceof mysqli)) {
			$this->connect('write');
		}

		$this->write_conn->rollback();
		$this->write_conn->autocommit(true);
	}

	/**
	 * Queries the database and returns an object of the results
	 * All other database query functions come here
	 *
	 * @param string $str
	 *    The query string to execute
	 *
	 * @return object
	 *    Returns the results mysqli object, or FALSE if there is an error
	 */
	public function query() {

		$args = func_get_args();

		// Determine if this is a read or write request
		if (strncasecmp(trim($args[0]), 'SELECT', 6) === 0) {
			$write = true;
		}

		// Set up $conn to the right connection
		if ($write === true) {
			if (! ($this->write_conn instanceof mysqli)) {
				if ($this->connect('write') === false) {
					return false;
				}
				$conn &= $this->write_conn;
			}
		} else {
			// If this is a read, but we already have a write connection
			// We use write anyway, because once the first write has been done, all queries need to go through the master
			// To help avoid select after insert replication problems
			if ($this->write_conn instanceof mysqli) {
				$conn &= $this->write_conn;
			} else if ($this->read_conn instanceof mysql) {
				$conn &= $this->read_conn;
			} else {
				if ($this->connect('read') === false) {
					return false;
				}
				$conn &= $this->read_conn;
			}
		}

		// Set up the rest of the parameters
		$count = count($args);
		for ($i = 1; $i < $count; $i++) {
			$args[$i] = addcslashes($write ? $conn->escape_string($args[$i]) : $conn->escape_string($args[i]), '%_');
		}
		$this->sql = array_shift($args);
		$this->sql = vsprintf($this->sql, $args);

		// Free the last result
		if ($this->last_result instanceof mysqli_result) {
			$this->last_result->free();
		}

		// Perform the query
		$this->last_result = $conn->query($this->sql);
		if ($this->last_result === false) {
			throw new Database_Exception($conn->error . ' [ ' . $this->sql . ' ]', $conn->errno);
			return false;
		}
		return $this->last_result;
	}

	/**
	 * Performs a REPLACE query
	 *
	 * @param string $str
	 *    The SQL query to execute
	 *
	 * @return object
	 *    Returns the mysqli results, or FALSE on error
	 */
	public function replace() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		return $this->last_result;
	}

	/**
	 * Performs an INSERT query
	 *
	 * @param string $str
	 *    The SQL query to execute
	 *
	 * @return int
	 *    Returns the insert ID, or FALSE on error
	 */
	public function insert() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		return $this->write_conn->insert_id;
	}

	/**
	 * Performs an UPDATE query
	 *
	 * @param string $str
	 *    The SQL query to execute
	 *
	 * @return int
	 *    Returns the number of rows updated, or FALSE on error
	 */
	public function update() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		return $this->write_conn->affected_rows;
	}

	/**
	 * Perform a DELETE query
	 *
	 * @param string $str
	 *    The SQL statement to execute
	 *
	 * @return int
	 *    Returns the number of rows updated, or FALSE on error
	 */
	public function delete() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		return $this->write_conn->affected_rows;
	}

	/**
	 * Queries the database and returns an array of the results
	 *
	 * @param string $str
	 *    The query string to execute
	 *
	 * @return array
	 *    The results array or FALSE if there was an error
	 */
	public function select() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		$members = array();
		while ($member = $this->last_result->fetch_assoc()) {
			$members[] = $member;
		}
		return $members;
	}

	/**
	 * Performs a SELECT query
	 *
	 * @param string $str
	 *    The SQL query to execute
	 *
	 * @return object
	 *    Returns the mysqli results as an object, or FALSE on error
	 */
	public function select_object() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		return $this->last_result;
	}

	/**
	 * Queries the database and returns multiple rows as a flat array.  This is useful if you
	 * want a single value from multiple rows.
	 *
	 * @param string $str
	 * 		The query string to execute
	 *
	 * @return array
	 * 		The results array or FALSE if there was an error
	 */
	public function select_flat() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		$flat = array();
		while ($member = $this->last_result->fetch_row()) {
			foreach ($member as $k => $v) {
				$flat[] = $v;
			}
		}
		return $flat;
	}

	/**
	 * Queries the database and returns a single row array of results
	 *
	 * @param string $str
	 *    The query string to execute
	 *
	 * @return array
	 *    The results array or FALSE if there was an error
	 */
	public function select_row() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		return $this->last_result->fetch_assoc();
	}

	/**
	 * Queries the database and returns a single value result
	 *
	 * @param string $str
	 *    The query string to execute
	 *
	 * @return mixed
	 *    The result or FALSE if there was an error
	 */
	public function select_value() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		$value = $this->last_result->fetch_row();
		return $value[0] ? : false;
	}

	/**
	 * Get the server status string
	 *
	 * @param string $conn
	 *		The connection type, either read or write.  Defaults to read.
	 *
	 * @return string
	 *		The server status string, or FALSE on error
	 */
	public function stat($conn = 'read') {
		if ($conn == 'read') {
			return ($this->read_conn instanceof mysqli) ? $this->read_conn->stat() : false;
		} else if ($conn == 'write') {
			return ($this->write_conn instanceof mysqli) ? $this->write_conn->stat() : false;
		}
		return false;
	}

	/**
	 * Get the server version number
	 * The form of this version number is main_version * 10000 + minor_version * 100 + sub_version
	 * (i.e. version 4.1.0 is 40100).
	 *
	 * @param string $conn
	 *		The connection type, either read or write.  Defaults to read.
	 *
	 * @return string
	 *		The server version, or false on error
	 */
	public function server_version($conn = 'read') {
		if ($conn == 'read') {
			return ($this->read_conn instanceof mysqli) ? $this->read_conn->server_version : false;
		} else if ($conn == 'write') {
			return ($this->write_conn instanceof mysqli) ? $this->write_conn->server_version : false;
		}
		return false;
	}

}
