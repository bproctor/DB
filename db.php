<?php

/**
 * db.php
 *
 * Copyright (c) 2010-2011 Brad Proctor. (http://bradleyproctor.com)
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @author      Brad Proctor
 * @copyright   Copyright (c) 2010-2011 Brad Proctor
 * @license     MIT License (http://www.opensource.org/licenses/mit-license.php)
 * @link        http://bradleyproctor.com/
 * @version     1.0
 */

class Database_Exception extends \FuelException { }

class DB {

    private $conn = null;			// The connection
    private $last_result;           // The last query result
    private $sql;                   // Last query


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
		static $instance;

		if (!($instance instanceof DB)) {
			$instance = new DB();
		}
		return $instance;
	}

	/**
	 * Connect to a database
	 *
	 * @param string $host
	 *		Hostname to connect to
	 *
	 * @param string $user
	 *		The database usename
	 *
	 * @param string $pass
	 *		The database password
	 *
	 * @param string $name
	 *		The database name
	 *
	 * @param int $port
	 *		The port to use to connect
	 *
	 * @param string $char
	 *		The character set
	 *
	 * @return bool
	 *		Returns TRUE on success, false on error
	 */
    public function connect($host = null, $user = null, $pass = null, $name = null, $port = null, $char = null) {
		try {
			$config = \Config::get('db.' . \Config::get('db.active'));
			$host or $host = $config[0]['hostname'];
			$user or $user = $config[0]['username'];
			$pass or $pass = $config[0]['password'];
			$name or $name = $config[0]['database'];
			$port or $port = $config[0]['port'];
			$char or $char = $config[0]['charset'];

			$this->conn = new mysqli($host, $user, $pass, $name, $port);
			if ($this->conn->error) {
				throw new Database_Exception($this->conn->error, $this->conn->errno);
			}

			if ($this->conn->set_charset($char) === false) {
				throw new Database_Exception($this->conn->error, $this->conn->errno);
			}
		} catch (ErrorException $e) {
            throw new Database_Exception('No MySQLi Connection', 0);
        }
        return $this->conn->error;
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
    public function close() {
        if ($this->conn instanceof mysqli) {
            return $this->conn->close();
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
		if ($this->conn instanceof mysqli) {
			return $this->conn->error;
		}
		return false;
    }

    /**
     * Free the memory from the last results
	 *
	 * @return bool
	 *		Returns TRUE if the result was successfully freed, FALSE on error
     */
    public function free() {
        if ($this->last_result instanceof mysqli) {
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
		if ($this->conn instanceof mysqli) {
			return $this->conn->insert_id;
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
	 *		Return the number of affected rows from last query, or FALSE
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
		if (! ($this->conn instanceof mysqli)) {
			$this->connect();
		}
		$this->conn->autocommit(false);
    }

	/**
     * Commit the current transaction
     */
	public function commit() {
		if (! ($this->conn instanceof mysqli)) {
			$this->connect();
		}

		$this->conn->commit();
		$this->conn->autocommit(true);
	}

	/**
     * Rollback a transaction
     */
    public function rollback() {
		if (! ($this->conn instanceof mysqli)) {
			$this->connect();
		}

        $this->conn->rollback();
		$this->conn->autocommit(true);
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

        // Allow for lazy connecting
        if ($this->conn == null) {
            if ($this->connect() === false) {
                return false;
            }
        }

        // Free the last result
        if ($this->last_result instanceof mysqli) {
            $this->last_result->free();
        }

        // Set up the arguments
        $args = func_get_args();
        $count = count($args);
        for ($i = 1; $i < $count; $i++) {
            $args[$i] = addcslashes($this->conn->escape_string($args[$i]), '%_');
        }
        $this->sql = array_shift($args);
        $this->sql = vsprintf($this->sql, $args);

        // Perform the query
        $this->last_result = $this->conn->query ($this->sql);
        if ($this->last_result === false) {
			throw new Database_Exception($this->conn->error.' [ '.$this->sql.' ]', $this->conn->errno);
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
        return $this->conn->insert_id;
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
        return $this->conn->affected_rows;
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
        return $this->conn->affected_rows;
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
	 *		The query string to execute
	 *
	 * @return array
	 *		The results array or FALSE if there was an error
	 */
	public function select_flat() {
		$args = func_get_args();
		if (call_user_func_array(array('DB', 'query'), $args) === false) {
			return false;
		}
		$flat = array();
		while ($members = $this->last_result->fetch_assoc()) {
			foreach ($members as $key => $value) {
				$flat[] = $value;
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
        $value = $this->last_result->fetch_array();
        return $value[0] ?: false;
    }

}

