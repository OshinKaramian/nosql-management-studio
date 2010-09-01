//
// redis-sharp.cs: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
//
// Modified by:
//  László Séra (http://www.aspninja.com)
//
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of reddis: new BSD license.
//
// 
#define DEBUG

using System;
using System.IO;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Diagnostics;

/// <summary>
/// 
/// </summary>
public class Redis : IDisposable
{
    #region members 
    
    Socket socket;
	BufferedStream bstream;
    int db;

    #endregion

    #region Object lifecycle

    /// <summary>
    /// Initializes a new instance of the <see cref="Redis"/> class.
    /// </summary>
    /// <param name="host">The host.</param>
    /// <param name="port">The port.</param>
    public Redis (string host, int port)
	{
		if (host == null)
			throw new ArgumentNullException ("host");
		
		Host = host;
		Port = port;
		SendTimeout = -1;
	}

    /// <summary>
    /// Initializes a new instance of the <see cref="Redis"/> class.
    /// </summary>
	public Redis () : this ("localhost", 6379)
	{
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases unmanaged resources and performs other cleanup operations before the
    /// <see cref="Redis"/> is reclaimed by garbage collection.
    /// </summary>
    ~Redis()
    {
        Dispose(false);
    }

    #endregion

    #region Properties

    public string Host { get; private set; }
	public int Port { get; private set; }
	public int RetryTimeout { get; set; }
	public int RetryCount { get; set; }
	public int SendTimeout { get; set; }
	public string Password { get; set; }
	
	public string this [string key] {
		get { return GetString (key); }
		set { Set (key, value); }
    }

    #endregion

    #region Connection handling

    /// <summary>
    /// simple password authentication if enabled
    /// </summary>
    void Connect()
    {
        socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.SendTimeout = SendTimeout;
        socket.Connect(Host, Port);
        if (!socket.Connected)
        {
            socket.Close();
            socket = null;
            return;
        }
        bstream = new BufferedStream(new NetworkStream(socket), 16 * 1024);

        if (Password != null)
            SendExpectSuccess("AUTH {0}\r\n", Password);
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources, close the connection
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            SendCommand("QUIT\r\n");
            socket.Close();
            socket = null;
        }
    }

    /// <summary>
    /// return the value of the key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public string Ping()
    {
        return SendExpectString("PING\r\n");
    }
	
	public string SendCommand(string command)
	{
		return Encoding.UTF8.GetString(SendExpectData(null, command + "\r\n"));	
	}

    #endregion

    #region Commands operating on all the kind of values

    /// <summary>
    /// test if a key exists
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns>
    /// 	<c>true</c> if the redis contains key; otherwise, <c>false</c>.
    /// </returns>
    public bool ContainsKey(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("EXISTS " + key + "\r\n") == 1;
    }

    /// <summary>
    /// Removes the specified key.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public bool Remove(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("DEL " + key + "\r\n", key) == 1;
    }

    /// <summary>
    /// Removes the specified args.
    /// </summary>
    /// <param name="args">The args.</param>
    /// <returns></returns>
    public int Remove(params string[] args)
    {
        if (args == null)
            throw new ArgumentNullException("args");
        return SendExpectInt("DEL " + string.Join(" ", args) + "\r\n");
    }

    /// <summary>
    /// return the type of the value stored at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public KeyType TypeOf(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        switch (SendExpectString("TYPE {0}\r\n", key))
        {
            case "none":
                return KeyType.None;
            case "string":
                return KeyType.String;
            case "set":
                return KeyType.Set;
            case "list":
                return KeyType.List;
        }
        throw new ResponseException("Invalid value");
    }

    /// <summary>
    /// Greturn all the keys
    /// </summary>
    /// <value>The keys.</value>
    public string[] Keys
    {
        get
        {
            return Encoding.UTF8.GetString(SendExpectData(null, "KEYS *\r\n")).Split(' ');
        }
    }

    /// <summary>
    /// return all the keys matching a given pattern
    /// </summary>
    /// <param name="pattern">The pattern.</param>
    /// <returns></returns>
    public string[] GetKeys(string pattern)
    {
        if (pattern == null)
            throw new ArgumentNullException("key");
        return Encoding.UTF8.GetString(SendExpectData(null, "KEYS {0}\r\n", pattern)).Split(' ');
    }

    /// <summary>
    /// return a random key from the key space
    /// </summary>
    /// <returns></returns>
    public string RandomKey()
    {
        return SendExpectString("RANDOMKEY\r\n");
    }

    /// <summary>
    /// rename the old key in the new one, destroing the newname key if it already exists
    /// </summary>
    /// <param name="oldKeyname">The old keyname.</param>
    /// <param name="newKeyname">The new keyname.</param>
    /// <returns></returns>
    public bool Rename(string oldKeyname, string newKeyname)
    {
        if (oldKeyname == null)
            throw new ArgumentNullException("oldKeyname");
        if (newKeyname == null)
            throw new ArgumentNullException("newKeyname");
        return SendGetString("RENAME {0} {1}\r\n", oldKeyname, newKeyname)[0] == '+';
    }

    /// <summary>
    ///  Rename oldkey into newkey but fails if the destination key newkey already exists.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public bool RenameNX(string oldKeyname, string newKeyname)
    {
        if (oldKeyname == null)
            throw new ArgumentNullException("oldKeyname");
        if (newKeyname == null)
            throw new ArgumentNullException("newKeyname");

        return SendExpectInt("RENAMENX {0} {1}\r\n", oldKeyname, newKeyname) == 1;
    }

    /// <summary>
    /// return the number of keys in the current db
    /// </summary>
    /// <value>number of keys in the current db</value>
    public int DbSize
    {
        get
        {
            return SendExpectInt("DBSIZE\r\n");
        }
    }

    /// <summary>
    /// Set a time to live in seconds on a key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="seconds">The seconds.</param>
    /// <returns></returns>
    public bool Expire(string key, int seconds)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("EXPIRE {0} {1}\r\n", key, seconds) == 1;
    }

    /// <summary>
    /// Set a time to live in seconds on a key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="time">unixtime</param>
    /// <returns></returns>
    public bool ExpireAt(string key, int time)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("EXPIREAT {0} {1}\r\n", key, time) == 1;
    }

    /// <summary>
    ///  get the time to live in seconds of a key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public int TimeToLive(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("TTL {0}\r\n", key);
    }

    /// <summary>
    /// Select the DB having the specified index
    /// </summary>
    /// <value>The db.</value>
    public int Db
    {
        get
        {
            return db;
        }

        set
        {
            db = value;
            SendExpectSuccess("SELECT {0}\r\n", db);
        }
    }

    #endregion

    #region Commands operating on string values

    /// <summary>
    /// set a key to a string value
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public bool Set(string key, string value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        return Set(key, Encoding.UTF8.GetBytes(value));
    }

    /// <summary>
    /// set a key to a value
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public bool Set(string key, byte[] value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        if (value.Length > 1073741824)
            throw new ArgumentException("value exceeds 1G", "value");

        if (!SendDataCommand(value, "SET {0} {1}\r\n", key, value.Length))
            throw new Exception("Unable to connect");
        ExpectSuccess();

        return true;
    }

    /// <summary>
    ///  set a key to a string value if the key does not exist
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public bool SetNX(string key, string value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        SetNX(key, Encoding.UTF8.GetBytes(value));

        return true;
    }

    /// <summary>
    /// return the value of the key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public byte[] Get(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectData(null, "GET " + key + "\r\n");
    }

    /// <summary>
    /// return the string value of the key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public string GetString(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return Encoding.UTF8.GetString(Get(key));
    }
	


    /// <summary>
    /// set a key to a string returning the old value of the key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <returns></returns>
    public byte[] GetSet(string key, byte[] value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        if (value.Length > 1073741824)
            throw new ArgumentException("value exceeds 1G", "value");

        if (!SendDataCommand(value, "GETSET {0} {1}\r\n", key, value.Length))
            throw new Exception("Unable to connect");

        return ReadData();
    }

    /// <summary>
    /// set a key to a string returning the old value of the key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <returns></returns>
    public string GetSet(string key, string value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");
        return Encoding.UTF8.GetString(GetSet(key, Encoding.UTF8.GetBytes(value)));
    }

    /// <summary>
    /// multi-get, return the strings values of the keys
    /// </summary>
    /// <param name="keys">The keys.</param>
    /// <returns></returns>
    public byte[][] GetKeys(params string[] keys)
    {
        if (keys == null)
            throw new ArgumentNullException("key1");
        if (keys.Length == 0)
            throw new ArgumentException("keys");

        if (!SendDataCommand(null, "MGET {0}\r\n", string.Join(" ", keys)))
            throw new Exception("Unable to connect");

        return ReadMultipleBulkData();
    }    

    /// <summary>
    ///  set a key to a string value if the key does not exist
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public void SetNX(string key, byte[] value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        if (value.Length > 1073741824)
            throw new ArgumentException("value exceeds 1G", "value");

        if (!SendDataCommand(value, "SETNX {0} {1}\r\n", key, value.Length))
            throw new Exception("Unable to connect");
        ExpectSuccess();
    }

    /// <summary>
    /// increment the integer value of key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public int Increment(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("INCR " + key + "\r\n");
    }

    /// <summary>
    /// Increment the integer value of key by integer
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="count">The count.</param>
    /// <returns></returns>
    public int Increment(string key, int count)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("INCRBY {0} {1}\r\n", key, count);
    }

    /// <summary>
    /// decrement the integer value of key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public int Decrement(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("DECR " + key + "\r\n");
    }

    /// <summary>
    /// decrement the integer value of key by integer
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="count">The count.</param>
    /// <returns></returns>
    public int Decrement(string key, int count)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("DECRBY {0} {1}\r\n", key, count);
    }

    /// <summary>
    /// TSelect the DB with having the specified zero-based numeric index. For default every new client connection is automatically selected to DB 0.
    /// </summary>
    /// <param name="index">The index.</param>
    public bool SelectDB(int index)
    {
        if (!SendCommand("SELECT {0}\r\n", index))
            throw new Exception("Unable to connect");
        ExpectSuccess();

        return true;
    }

    /// <summary>
    /// Move the key from the currently selected DB to the DB having as index dbindex
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param index="key">The index.</param>
    public bool Move(string key, int index)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        return SendDataExpectInt(null, "MOVE {0} {1}\r\n", key, index) == 1;
    }

    #endregion

    #region Commands operating on lists

    /// <summary>
    ///  Append an element to the head/tail of the List value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="tail">if set to <c>true</c> [tail].</param>
    public bool Push(string key, string value, bool tail)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        Push(key, Encoding.UTF8.GetBytes(value), tail);

        return true;
    }

    /// <summary>
    /// Append an element to the head/tail of the List value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="tail">if set to <c>true</c> [tail].</param>
    public bool Push(string key, byte[] value, bool tail)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        if (value.Length > 1073741824)
            throw new ArgumentException("value exceeds 1G", "value");

        if (!SendDataCommand(value, (tail ? "RPUSH" : "LPUSH") + " {0} {1}\r\n", key, value.Length))
            throw new Exception("Unable to connect");
        ExpectSuccess();

        return true;
    }

    /// <summary>
    ///  Append an element to the head/tail of the List value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="tail">if set to <c>true</c> [tail].</param>
    public bool ListSet(string key, string value, int index)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        ListSet(key, Encoding.UTF8.GetBytes(value), index);

        return true;
    }

    /// <summary>
    /// Append an element to the head/tail of the List value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="tail">if set to <c>true</c> [tail].</param>
    public bool ListSet(string key, byte[] value, int index)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        if (value.Length > 1073741824)
            throw new ArgumentException("value exceeds 1G", "value");

        if (!SendDataCommand(value, "LSET {0} {1} {2}\r\n", key, index, value.Length))
            throw new Exception("Unable to connect");
        ExpectSuccess();

        return true;
    }

    /// <summary>
    /// Return the length of the list stored at the specified key.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public int GetListLength(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("LLEN {0} \r\n", key);
    }

    /// <summary>
    /// Return a range of elements from the List at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="start">The start.</param>
    /// <param name="end">The end.</param>
    /// <returns></returns>
    public List<string> GetListRangeString(string key, int start, int end)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        byte[][] items = GetListRange(key, start, end);
        List<string> result = new List<string>();

        if (items == null || items.Length == 0)
            return result;

        foreach (byte[] item in items)
        {
            result.Add(Encoding.UTF8.GetString(item));
        }
        return result;        
    }

    /// <summary>
    /// Return a range of elements from the List at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="start">The start.</param>
    /// <param name="end">The end.</param>
    /// <returns></returns>
    public byte[][] GetListRange(string key, int start, int end)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (!SendDataCommand(null, "LRANGE {0} {1} {2}\r\n", key, start, end))
            throw new Exception("Unable to connect");

        return ReadMultipleBulkData();
    }
    
    /// <summary>
    /// Return the element at index position from the List at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public byte[] ListIndex(string key, int index)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectData(null, "LINDEX  " + key + " " + index.ToString() + "\r\n");
    }

    /// <summary>
    /// Return the element at index position from the List at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public string ListIndexString(string key, int index)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return Encoding.UTF8.GetString(ListIndex(key, index));
    }

    /// <summary>
    /// return the value of the key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public byte[] Pop(string key, bool tail)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectData(null, (tail ? "RPOP" : "RPOP") + " " + key + "\r\n");
    }

    /// <summary>
    /// Return and remove (atomically) the first/last element of the List at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public string PopString(string key, bool tail)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return Encoding.UTF8.GetString(Pop(key, tail));
    }

    /// <summary>
    /// Trim the list at key to the specified range of elements
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="start">The start.</param>
    /// <param name="end">The end.</param>
    public bool TrimList(string key, int start, int end)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (!SendCommand("LTRIM {0} {1} {2}\r\n", key, start, end))
            throw new Exception("Unable to connect");
        ExpectSuccess();

        return true;
    }

    #endregion

    #region Commands operating on sets

    /// <summary>
    /// Add the specified value to the Set value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public bool SetAdd(string key, string value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        SetAdd(key, Encoding.UTF8.GetBytes(value));

        return true;
    }

    /// <summary>
    /// Add the specified value to the Set value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public bool SetAdd(string key, byte[] value)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        if (value == null)
            throw new ArgumentNullException("value");

        if (value.Length > 1073741824)
            throw new ArgumentException("value exceeds 1G", "value");

        if (!SendDataCommand(value, "SADD {0} {1}\r\n", key, value.Length))
            throw new Exception("Unable to connect");
        ExpectSuccess();

        return true;
    }

    /// <summary>
    /// Remove the specified member from the set value stored at key.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns>returns true if the new element was removed, otherwise false</returns>
    public bool SetRemove(string key, string value)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (value == null)
            throw new ArgumentNullException("value");

        return SetRemove(key, Encoding.UTF8.GetBytes(value));
    }

    /// <summary>
    /// Remove the specified member from the set value stored at key.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns>returns true if the new element was removed, otherwise false</returns>
    public bool SetRemove(string key, byte[] value)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (value == null)
            throw new ArgumentNullException("value");

        return SendDataExpectInt(value, "SREM {0} {1}\r\n", key, value.Length) == 1;
    }

    /// <summary>
    /// Return the number of elements (the cardinality) of the Set at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public int GetSetCardinality(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");
        return SendExpectInt("SCARD {0} \r\n", key);
    }

    /// <summary>
    ///  Test if the specified value is a member of the Set at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <returns></returns>
    public bool IsSetMember(string key, string member)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (member == null)
            throw new ArgumentNullException("member");

        return IsSetMember(key, Encoding.UTF8.GetBytes(member));
    }

    /// <summary>
    ///  Return the intersection between the Sets stored at key1, key2, ..., keyN
    /// </summary>
    /// <param name="keys">The keys.</param>
    /// <returns></returns>
    public List<string> GetSetInterString(params string[] keys)
    {
        if (keys == null)
            throw new ArgumentNullException("key1");
        if (keys.Length == 0)
            throw new ArgumentException("keys");

        List<string> result = new List<string>();
        byte[][] items = GetSetInter(keys);
        if (items == null || items.Length == 0)
            return result;

        foreach (byte[] item in items)
        {
            result.Add(Encoding.UTF8.GetString(item));
        }
        return result;        
    }

    /// <summary>
    ///  Return the intersection between the Sets stored at key1, key2, ..., keyN
    /// </summary>
    /// <param name="keys">The keys.</param>
    /// <returns></returns>
    public byte[][] GetSetInter(params string[] keys)
    {
        if (keys == null)
            throw new ArgumentNullException("key1");
        if (keys.Length == 0)
            throw new ArgumentException("keys");

        if (!SendDataCommand(null, "SINTER {0}\r\n", string.Join(" ", keys)))
            throw new Exception("Unable to connect");

        return ReadMultipleBulkData();
    }

    /// <summary>
    ///  Test if the specified value is a member of the Set at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <returns></returns>
    public bool IsSetMember(string key, byte[] member)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (member == null)
            throw new ArgumentNullException("member");

        return SendDataExpectInt(member, "SISMEMBER {0} {1}\r\n", key, member.Length) == 1;
    }

    /// <summary>
    /// Return all the members of the Set value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public List<string> GetSetMembersString(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        byte[][] items = GetSetMembers(key);
        List<string> result = new List<string>();

        if (items == null || items.Length == 0)
            return result;
        
        foreach (byte[] item in items)
        {
            result.Add(Encoding.UTF8.GetString(item));
        }
        return result;
    }

    /// <summary>
    /// Return all the members of the Set value at key
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns></returns>
    public byte[][] GetSetMembers(string key)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (!SendDataCommand(null, "SMEMBERS {0} \r\n", key))
            throw new Exception("Unable to connect");

        return ReadMultipleBulkData();
    }

    #endregion

    #region Commands operating on sorted sets (zsets, Redis version >= 1.1)

    #endregion

    #region Sorting

    public List<string> SortString(string key, string query)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        byte[][] items = Sort(key, query);
        if (items == null || items.Length == 0)
            return null;

        List<string> result = new List<string>();
        foreach (byte[] item in items)
        {
            result.Add(Encoding.UTF8.GetString(item));
        }
        return result;
    }

    /// <summary>
    /// Sort a Set or a List accordingly to the specified parameters
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="query">The query.</param>
    /// <returns></returns>
    public byte[][] Sort(string key, string query)
    {
        if (key == null)
            throw new ArgumentNullException("key");

        if (!SendDataCommand(null, "SORT {0} {1}\r\n", key, query))
            throw new Exception("Unable to connect");

        return ReadMultipleBulkData();
    }

    #endregion

    #region Persistence control commands

    /// <summary>
    /// Synchronously save the DB on disk
    /// </summary>
    /// <returns></returns>
    public string Save()
    {
        return SendGetString("SAVE\r\n");
    }

    /// <summary>
    /// Asynchronously save the DB on disk
    /// </summary>
    public string BackgroundSave()
    {
        return SendGetString("BGSAVE\r\n");
    }

    /// <summary>
    /// Synchronously save the DB on disk, then shutdown the server.
    /// </summary>
    public string Shutdown()
    {
        return SendGetString("SHUTDOWN\r\n");
    }

    const long UnixEpoch = 621355968000000000L;

    /// <summary>
    /// Return the UNIX time stamp of the last successfully saving of the dataset on disk
    /// </summary>
    /// <value>The last save.</value>
    public DateTime LastSave
    {
        get
        {
            int t = SendExpectInt("LASTSAVE\r\n");

            return new DateTime(UnixEpoch) + TimeSpan.FromSeconds(t);
        }
    }

    #endregion

    #region Remote server control commands

    /// <summary>
    /// Provide information and statistics about the server
    /// </summary>
    /// <returns></returns>
    public Dictionary<string, string> GetInfo()
    {
        byte[] r = SendExpectData(null, "INFO\r\n");
        var dict = new Dictionary<string, string>();

        foreach (var line in Encoding.UTF8.GetString(r).Split('\n'))
        {
            int p = line.IndexOf(':');
            if (p == -1)
                continue;
            dict.Add(line.Substring(0, p), line.Substring(p + 1));
        }
        return dict;
    }

    #endregion

    #region Communication

    byte[] end_data = new byte[] { (byte)'\r', (byte)'\n' };

	string ReadLine ()
	{
		var sb = new StringBuilder ();
		int c;
		
		while ((c = bstream.ReadByte ()) != -1){
			if (c == '\r')
				continue;
			if (c == '\n')
				break;
			sb.Append ((char) c);
		}
		return sb.ToString ();
	}
	
   	bool SendDataCommand (byte [] data, string cmd, params object [] args)
	{
		if (socket == null)
			Connect ();
		if (socket == null)
			return false;

		var s = args.Length > 0 ? String.Format (cmd, args) : cmd;
		byte [] r = Encoding.UTF8.GetBytes (s);
		try {
			Log ("S: " + String.Format (cmd, args));
			socket.Send (r);
			if (data != null){
				socket.Send (data);
				socket.Send (end_data);
			}
		} catch (SocketException){
			// timeout;
			socket.Close ();
			socket = null;

			return false;
		}
		return true;
	}

	bool SendCommand (string cmd, params object [] args)
	{
		if (socket == null)
			Connect ();
		if (socket == null)
			return false;

		var s = args != null && args.Length > 0 ? String.Format (cmd, args) : cmd;
		byte [] r = Encoding.UTF8.GetBytes (s);
		try {
			Log ("S: " + String.Format (cmd, args));
			socket.Send (r);
		} catch (SocketException){
			// timeout;
			socket.Close ();
			socket = null;

			return false;
		}
		return true;
	}
	
	[Conditional ("DEBUG")]
	void Log (string fmt, params object [] args)
	{
		Console.WriteLine ("{0}", String.Format (fmt, args).Trim ());
	}

	void ExpectSuccess ()
	{
		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		var s = ReadLine ();
		Log ((char)c + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR") ? s.Substring (4) : s);
	}
	
	void SendExpectSuccess (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		ExpectSuccess ();
	}

    int SendDataExpectInt(byte[] data, string cmd, params object[] args)
    {
        if (!SendDataCommand (data, cmd, args))
			throw new Exception ("Unable to connect");

        int c = bstream.ReadByte();
        if (c == -1)
            throw new ResponseException("No more data");

        var s = ReadLine();
        Log("R: " + s);
        if (c == '-')
            throw new ResponseException(s.StartsWith("ERR") ? s.Substring(4) : s);
        if (c == ':')
        {
            int i;
            if (int.TryParse(s, out i))
                return i;
        }
        throw new ResponseException("Unknown reply on integer request: " + c + s);
    }

	int SendExpectInt (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		var s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR") ? s.Substring (4) : s);
		if (c == ':'){
			int i;
			if (int.TryParse (s, out i))
				return i;
		}
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
	}	

	string SendExpectString (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		var s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR") ? s.Substring (4) : s);
		if (c == '+')
			return s;
		
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
	}	

	//
	// This one does not throw errors
	//
	string SendGetString (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		return ReadLine ();
	}	
	
	byte[] SendExpectData (byte[] data, string cmd, params object [] args)
	{
		if (!SendDataCommand (data, cmd, args))
			throw new Exception ("Unable to connect");

		return ReadData ();
	}

    byte[][] ReadMultipleBulkData()
    {
        int c = bstream.ReadByte();
        if (c == -1)
            throw new ResponseException("No more data");

        var s = ReadLine();
        Log("R: " + s);
        if (c == '-')
            throw new ResponseException(s.StartsWith("ERR") ? s.Substring(4) : s);
        if (c == '*')
        {
            int count;
            if (int.TryParse(s, out count))
            {
                if (count == -1)
                    return null;

                byte[][] result = new byte[count][];

                for (int i = 0; i < count; i++)
                    result[i] = ReadData();

                return result;
            }

            throw new ResponseException("Invalid length");
        }

        throw new ResponseException("Unexpected reply: " + s);
    }

	byte[] ReadData()
	{
		string r = ReadLine ();
		Log ("R: {0}", r);
		if (r.Length == 0)
			throw new ResponseException ("Zero length respose");
		
		char c = r [0];
		if (c == '-')
			throw new ResponseException (r.StartsWith ("-ERR") ? r.Substring (5) : r.Substring (1));
		if (c == '$'){
			if (r == "$-1")
				return null;
			int n;
			
			if (Int32.TryParse (r.Substring (1), out n)){
				byte [] retbuf = new byte [n];
				bstream.Read (retbuf, 0, n);
				if (bstream.ReadByte () != '\r' || bstream.ReadByte () != '\n')
					throw new ResponseException ("Invalid termination");
				return retbuf;
			}
			throw new ResponseException ("Invalid length");
		}
		throw new ResponseException ("Unexpected reply: " + r);
    }

    #endregion

    public enum KeyType
    {
        /// <summary>
        /// {35A90EBF-F421-44A3-BE3A-47C72AFE47FE}
        /// </summary>
        None, String, List, Set
    }

    public class ResponseException : Exception
    {
        public ResponseException(string code) : base("Response error")
        {
            Code = code;
        }

        public string Code { get; private set; }
    }	
}

