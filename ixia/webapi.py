import cookielib
import json
import requests
import string
import time
import httplib
import textwrap
import uuid
import threading
import copy

from copy import deepcopy
from urlparse import urljoin

# a list of supported scriptapi versions. 
kSupportedScriptApiVersions = ['v1']

# Properties that require renaming on the way in.
#   Sometimes json includes names that mess up our python, so we rename them to something safe,
#   and then rename them back before we use them
kJsonPropertyRenameMap = { "self": "_self_", "$type": "_type_"}
# A map back the other way
kJsonRenamedPropertyMap = {}
for key, value in kJsonPropertyRenameMap.iteritems():
    kJsonRenamedPropertyMap[value] = key

def waitForProperty(obj, propertyName, targetValues, validValues=[], invalidValues=[], timeout=None, trace=False):
    """Utility method to wait for a property on an object to change to an expected value.

    Object must support getattr and implement the httpRefresh() method. Object is polled once per second.
    @param obj: the object to query
    @param propertyName: the name of the property to check
    @param targetValues: a collection with the list of values for propertyName to wait for
    @param validValues: a collection with a list of values for propertyName that are allowed (any other causes exception)
    @param invalidValues: a collection with a list of values for propertyName that cause an exception to be raised
    @param timeout: max number of seconds to wait
    @param trace: if True, then the property value is printed out each polling cycle.
    """
    startTime = time.time()
    while True:
        obj.httpRefresh()
        value = getattr(obj, propertyName)
        if trace:
            print "property %s = %s" % (propertyName, value)
        if value in targetValues:
            return
        if (validValues and value not in validValues) or value in invalidValues:
            raise WebException("waitForProperty(): %s has invalid %s == %s" % (obj.__class__, propertyName, value))
        if timeout and time.time()-startTime > timeout:
            raise WebApiTimeout("waitForProperty(): %s timed out waiting for %s in %s" % (obj.__class__, propertyName, targetValues))
        time.sleep(1)

def checkForPropertyValue(obj, propertyName, expectedValues, refresh=False):
    """Utility method to check if a property on an object has one of the expected values.
    @param obj: the object to query
    @param propertyName: the name of the property to check
    @param expectedValues: a collection with the list of values for propertyName to check
    @param refresh: if True, then the object is refreshed using its httpRefresh() method
    """
    if refresh:
        obj.httpRefresh()
    value = getattr(obj, propertyName)
    if value in expectedValues:
        return True
    return False

class Validators(object):
    """A set of static validator methods for use in this module.
    """

    kFormatIsRequired = "The '%s' parameter is required."
    kFormatRequiresType = "The '%s' parameter requires %s. Got a(n) %s: %s."

    @staticmethod
    def checkString(param, paramName):
        if not isinstance(param, basestring):
            raise ValueError(Validators.kFormatRequiresType % (paramName, 'a string', type(param), param))

    @staticmethod
    def checkNonEmptyString(param, paramName):
        Validators.checkString(param, paramName)
        if not param:
            raise ValueError(Validators.kFormatIsRequired % paramName)

    @staticmethod
    def checkNotNone(param, paramName):
        if param is None:
            raise ValueError("The '%s' parameter may not be None." % paramName)

    @staticmethod
    def checkSessionType(sessionType, paramName="sessionType"):
        Validators.checkNonEmptyString(sessionType, paramName)

    @staticmethod
    def checkConfigName(configName, paramName="configName"):
        Validators.checkNonEmptyString(configName, paramName)

    @staticmethod
    def checkFile(fileParam, paramName):
        if isinstance(fileParam, basestring):
            raise ValueError(Validators.kFormatRequiresType % (paramName, 'a file handle', type(fileParam), fileParam))
        if not fileParam:
            raise ValueError(Validators.kFormatIsRequired % paramName)

    @staticmethod
    def checkInt(intParam, paramName):
        try:
            int(intParam)
        except:
            raise ValueError("The '%s' parameter must be an integer. Was %s." % (paramName, intParam))
    
    @staticmethod
    def checkLong(longParam, paramName):
        try:
            long(longParam)
        except:
            raise ValueError("The '%s' parameter must be a long. Was %s." % (paramName, longParam))

    @staticmethod
    def checkList(param, paramName):
        if not isinstance(param, list):
            raise ValueError(Validators.kFormatRequiresType % (paramName, 'a list', type(param), param))


class _JsonEncoder(json.JSONEncoder):
    """Internal class to expose an object's __dict__ as the json representation."""
    def default(self, obj):
        if isinstance(obj, dict) or isinstance(obj, list):
            return super(_JsonEncoder, self).default(obj)
        # proxy object
        return obj._jsonProperties_


class WebObjectLocation(object):
    """An object represent the source of a web object.

    This is populated by the webApi get and post 
    methods, and later by put() method to return
    the object from whence it came.
    """
    kLinksParam = "links"
    kEmbeddedParam = "embedded"
    
    def __init__(self, convention, url, *urlExts):
        Validators.checkNotNone(convention, "convention")
        Validators.checkString(url, "url")
        for urlExt in urlExts:
            url = HttpConvention.urljoin(url, urlExt)
        self.convention = convention
        self.url = url

    def httpPut(self, target):
        """Put target object back to the contained location"""
        self.convention.httpPut(self.url, target)

    def httpPatch(self, target):
        """Patch the target object on the contained location"""
        self.convention.httpPatch(self.url, target)

    def httpGet(self):
        """Regets the object from the original location and returns it (as a WebObject)."""
        return self.convention.httpGet(self.url)

    def httpGetProperty(self, url):
        """Gets a non-shallow property of this object"""
        return self.convention.httpGet(url, params={self.kLinksParam:True, self.kEmbeddedParam:False})

    def httpDelete(self):
        """Delete the current object object from the web server"""
        self.convention.httpDelete(self.url)

class WebObjectBase(object):
    """ The base class for Json Proxy object and list.

    Note that internal and seldom-used method names are embedded in underscores ("_")
    to help prevent name collosions with properties defined in proxied JSON text.
    """
    kLockedProperty = "_locked_"
    kSourceProperty = "_source_"
    kLinksProperty = "links"
    kSelfLink = "self"
    kNonJsonProperties = [kLockedProperty, kSourceProperty]

    def __init__(self, source=None):
        self._locked_ = False
        self._setSource_(source)

    def __setattr__(self, propertyName, value):
        """Internal method to implement lock/unlock."""
        if propertyName != WebObjectBase.kLockedProperty and self._locked_ and propertyName not in self.__dict__:
            raise KeyError("Cannot define new property when Json proxy is locked. Proxies may be unlocked and relocked using _unlock_() and _lock_() methods.")
        return super(WebObjectBase, self).__setattr__(propertyName, value)

    def __getattr__ (self, propertyName):
        """Internal method to automatically request data from web server for shallow web objects"""
        if self._source_:
            try:
                links = super(WebObjectBase, self).__getattribute__(WebObjectBase.kLinksProperty)
            except AttributeError:
                return super(WebObjectBase, self).__getattribute__(propertyName)
        
            for link in links:
                if link.rel == propertyName:
                    result = self._source_.httpGetProperty(link.href)
                    self._setNewField(propertyName, result)
                    return result
                    
        return super(WebObjectBase, self).__getattribute__(propertyName)

    @property
    def _json_(self):
        """Returns the json representation (in lists and dictionaries) for the WebObject."""
        return json.loads(str(self))

    @property    
    def _pretty_(self):
        return json.dumps(self._json_, sort_keys=True, indent=4, separators=(',', ': '))

    def __str__(self):
        """Convenience override of str() method to returns the json representation for the object."""
        return json.dumps(self, cls=_JsonEncoder)

    @property
    def _jsonProperties_(self):
        properties = self.__dict__.copy()
        for propertyName in WebObjectBase.kNonJsonProperties:
            del properties[propertyName]
        return properties
    
    def _setNewField(self, fieldName, value):
        """Unlock the object if needed and add a new field"""
        if self._locked_:
            self._unlock_()
            try:
                self.__setattr__(fieldName, value)
            finally:
                self._lock_()
        else:
            self.__setattr__(fieldName, value)
        
    def _setNewFieldLock_(self, value):
        """set the property-creation lock.

        @param value: the new value of the property-creation lock
        """
        self._locked_ = value

    def _lock_(self):
        """Lock this object against creating fields by assigning to non-existent properties."""
        self._setNewFieldLock_(True)

    def _unlock_(self):
        """Unlock this object, allowing new fields to be created by assigning to non-existent properties."""
        self._setNewFieldLock_(False)

    def _setSource_(self, source):
        """Set the web location to send this object back to when httpPut() or httpPatch() is called

           @param source: The origin of the object
        """
        self._source_ = source

    def httpPut(self):
        """Put this object back to it's source.

        Note that this is only valid for WebObjects initialized with a source.
        The get and post methods automatically set these values
        """
        if not self._source_:
            raise WebException("WebObject.httpPut(): cannot put back to source because source was not specified when object was created")
        self._source_.httpPut(self)

    def httpPatch(self):
        """Patch the source of this object.

        Note that this is only valid for WebObjects initialized with a source.
        The get and post methods automatically set these values
        """
        if not self._source_:
            raise WebException("WebObject.httpPatch(): cannot put back to source because source was not specified when object was created")
        self._source_.httpPatch(self)

    def httpRefresh(self):
        """Updates the state of this object from it's source url."""
        if not self._source_:
            raise WebException("WebObject.httpRefresh(): cannot refresh because source was not specified when object was created")
        
        newData = self._source_.httpGet()
        try:
            for link in self.links:
                if link.rel in self.__dict__:
                    del self.__dict__[link.rel]
        except AttributeError:
            pass

        self.__dict__.update(newData.__dict__)

    def httpDelete(self):
        """Delete the source of this object.

        Note that this is only valid for WebObjects initialized with a source.
        The get and post methods automatically set these values
        """
        if not self._source_:
            raise WebException("WebObject.httpDelete(): cannot delete object from source because source was not specified when object was created")
        self._source_.httpDelete()
        self._source_ = None
	
    def getUrl(self):
        """Returns the url of this object."""
        if not self._source_:
            raise WebException("WebObject.getUrl(): cannot retrieve url because source was not specified when object was created")
		
        return self._source_.url


class WebObjectProxy(WebObjectBase):
    """An element of a tree of json proxy objects that represents an object/dictionary."""
    def __init__(self, _source_=None, **entries):
        super(WebObjectProxy, self).__init__(_source_)
        for key, value in entries.iteritems():
            # rename the properties back to their original
            key = kJsonRenamedPropertyMap.get(key, key)
            self.__dict__[key] = _WebObject(value)

    def __repr__(self):
        """Convenience override of repr() method to aid in debugging."""
        return "Object %s: %s" % (hex(id(self)), self)

    @property
    def objectType(self):
        """Gets the type of the object. Value is different than 'None' for polymorphic web services. Reads the '$type' json field."""
        try:
            type = getattr(self, "$type")
        except AttributeError:
            return None
        return type

    @objectType.setter
    def objectType(self, value):
        """Set the type of the object. Needs to be specified for polymorphic web services. It will add in json a '$type' field."""
        setattr(self, "$type", value)


class WebListProxy(WebObjectBase, list):
    """An element of a tree of json proxy objects that represents a list."""
    def __init__(self, entries=[], source=None):
        for item in entries:
            self.append(_WebObject(item))
        super(WebListProxy, self).__init__(source)

    def _checkArgs_(self, callName, item, kwArgs):
        """Checks the args and returns an item."""
        if item is None and not kwArgs:
            raise WebException("%s: Either a single unnamed parameter or a set of named parameters is required." % callName)
        if item is not None and kwArgs:
            raise WebException("%s: Either a single unnamed parameter or a set of named parameters are allowed, not both." % callName)
        if item is not None:
            item = _WebObject(item)
        if kwArgs:
            item = _WebObject(kwArgs)
        return item

    def __setitem__(self, idx, item):
        list.__setitem__(self, idx, item)

    def append(self, item=None, **kwArgs):
        """Append a WebObject to a WebObject list."""
        itemToAppend = self._checkArgs_("WebListProxy.append()", item, kwArgs)
        list.append(self, itemToAppend)

    def insert(self, idx, item=None, **kwArgs):
        itemToInsert = self._checkArgs_("WebListProxy.insert()", item, kwArgs)
        list.insert(self, idx, itemToInsert)

    def __repr__(self):
        """Convenience override of repr() method to aid in debugging."""
        return "List %s: %s" % (hex(id(self)), self)

    def _setSource_(self, source):
        super(WebListProxy, self)._setSource_(source)
        if not source:
            return
        for item in self:
            if not hasattr(item, self.kLinksProperty):
                continue
            for link in item.links:
                if link.rel == self.kSelfLink:
                    itemSource = WebObjectLocation(source.convention, link.href)
                    item._setSource_(itemSource)
                    break

def _WebObject(value):
    """Helper factory method for nested objects"""
    if isinstance(value, WebObjectBase):
        result = value
    elif isinstance(value, dict):
        for name, rename in kJsonPropertyRenameMap.iteritems():
            if name in value:
                value[rename] = value[name]
                del value[name]
        result = WebObjectProxy(**value)
        result._lock_()
    elif isinstance(value, list):
        result = WebListProxy(value)
        result._lock_()
    else:
        # simple types are directly represented as themselves
        result = value
    return result


def WebObject(*args, **kwArgs):
    """Factory method that converts dictionaries and lists into json object proxies.
        This is useful to call on a response that is a known json object, in which case it returns a locked proxy object that
        can conveniently access the json fields using python object/list notation. It is locked to avoid accidentally creating
        new fields. The called can always call unlock on the returned object if adding fields is desired.

        @param args: zero or one positional parameter. If one, then the parameter
                     is a dictionary or list (possibly with nested dictionaries and lists) from which to create an 
                     equivalent python object
                     If no positional parameters are specified, then kwArgs is used to create the object. 
        @param kwArgs: if no positional argument is specified, then named parameters can be used to specify a json object
    """
    numArgs = len(args)
    if numArgs > 1:
        raise WebException("WebObject can accept at most one positional parameter")
    if numArgs == 1:
        if kwArgs:
            raise WebException("WebObject can accept either a value or a set of named parameters, but not both")
        value = args[0]
    if numArgs == 0:
        # note if neither value nor dictionary is specified then the result will be an empty object
        value = kwArgs
    return _WebObject(value)


def WebObjectWithSource(value, source, **kwArgs):
    """Factory method to create a web object and set its source location."""
    result = WebObject(value, **kwArgs)
    result._setSource_(source)
    return result


class WebException(Exception):
    """Exception for nonstandard exceptions thrown by webapi module"""
    def __init__(self, description="", result=None, extra=""):
        super(Exception, self).__init__(description+extra)
        self.result = result

    def getResult(self):
        """Returns the result of the query that resulted in the exception."""
        return self.result


class WebApiTimeout(WebException):
    """Specialized exception for timeouts"""
    pass

class StatsTimeoutException(WebException):
    """ Specialized exception for timeouts when getting stats values """
    pass

class HttpConvention(object):
    """Class for remembering a set of common headers, urlParameters, and cookies used by an HTTP conversation.
        @param url:     the base url. The url passed into the specific methods will be appended to this one 
        @param parentConvention: (optional) an HttpConvention that is extended by this one
        @param params:  (optional) a dictionary of name value pairs to send as URL urlParameters
        @param headers: (optional) a dictionary of name value pairs to send as HTTP headers
    """
    kMethodDelete = "DELETE"
    kMethodGet = "GET"
    kMethodHead = "HEAD"
    kMethodOptions = "OPTIONS"
    kMethodPost = "POST"
    kMethodPut = "PUT"
    kMethodPatch = "PATCH"
    kMethodTrace = "TRACE"

    kStandardStreamingChunkSize = 10240

    def __init__(self, url, parentConvention=None, params={}, headers={}, **kwArgs):
        self.url = url
        self.parentConvention = parentConvention
        self.params = params.copy()
        self.headers = headers.copy()
        self.cookies = cookielib.CookieJar();
        self.extras = kwArgs

    def updateHeaders(self, headerDict):
        """Add or change HTTP headers used for all operations by this object."""
        self.headers.update(headerDict)

    def updateParams(self, paramDict):
        """Add or change URL parameters used for all operations by this object."""
        self.params.update(paramDict)

    def getErrorNotifications(self):
        """Returns any notifications for errors.

        This base class method is a hook for subclasses to implement if their supported server(s) can provide
        additional error information via another API. When checking for notifications, the implementing subclass
        must pass false for the checkNotifications parameter in any calls that make HTTP requests (to prevent an infinite
        loop if the call to checkNotifications fails).
        """
        return []

    def _getFormattedErrorNotifications(self):
        """Utility method checks the notifications and throws a WebException or returns a string representation of notifications if no error.

        @return Either an empty string, or a newline-prefixed string representing the list of non-error notifications.
        @raises WebException
        """
        result = ""
        errors = self.getErrorNotifications()
        if errors:
            result = "\nError notification(s): " + str([error.message for error in errors])
        return result

    def check(self, result, method, originalUrl, checkNotifications):
        """Check an HTTP result and throw an exception if there was an error."""
        content = ""
        notifications = ""
        if not result.ok:
            if checkNotifications:
                notifications = self._getFormattedErrorNotifications()
            if result.text:
                content = "\n" + result.text
            raise WebException("%s request to '%s' failed: server returned status %s: %s%s%s" % (method, result.url, result.status_code, result.reason, content, notifications), result)
        if result.url != originalUrl and result.url.endswith("/login"):
            if checkNotifications:
                notifications = self._getFormattedErrorNotifications()
            raise WebException("%s request to '%s' failed: invalid URL or user key.%s" % (method, originalUrl, notifications))

    def httpRequest(self, method, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Utility method to send an HTTP request of any kind to self.url + url (url parameter).

        Throws an exception on any error, or return a Requests library result object.
        @param method: the HTTP method to use
        @param url: the url-relative url to send the request to.
        @param data: the data to send as the body of the HTTP request
        @param params: a dictionary with URL parameters to use for this message only
        @param headers: a dictionary with HTTP headers to use for this message only
        @param checkNotifications: a flag to indicate whether the server notifications should be checked on an HTTP error.
                                    This is normally set to False only when checking notifications to avoid infinite recursion.
        @param kwArgs: a dictionary with any other options to pass to the Requests library request call
        """
        Validators.checkNotNone(method, "method")
        params = self.resolveParams(params)
        headers = self.resolveHeaders(headers)
        absUrl = self.resolveUrl(url)
        # set verify to False to turn off SSL certificate validation 
        extras = {"verify":False}
        extras.update(self.resolveExtras(kwArgs))
        result = requests.request(method, absUrl, data=str(data), params=params, headers=headers, cookies=self.cookies, **extras)
        self.check(result, method, absUrl, checkNotifications)
        return result

    def resolveParams(self, params={}):
        # Internal method to merage all the conventions' params together prior to making the final HTTP request
        result = self.parentConvention and self.parentConvention.resolveParams() or {}
        result.update(self.params)
        result.update(params)
        return result

    def resolveHeaders(self, headers={}):
        # Internal method to merage all the conventions' headers together prior to making the final HTTP request
        result = self.parentConvention and self.parentConvention.resolveHeaders() or {}
        result.update(self.headers)
        result.update(headers)
        return result

    def resolveExtras(self, extras={}):
        # Internal method to merage all the conventions' extra Requests.request kwArgs together prior to making the final HTTP request
        result = self.parentConvention and self.parentConvention.resolveExtras() or {}
        result.update(self.extras)
        result.update(extras)
        return result

    def resolveUrl(self, url):
        """Get the absolute URL from the url for self and the specified url parameter.

        @param url: the relative url (even if starting with /) to make absolute.
        """
        Validators.checkString(url, "url")
        result = self.parentConvention and self.parentConvention.resolveUrl(self.url) or self.url
        return HttpConvention.urljoin(result, url)

    def httpDelete(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Deletes the resource associated with self.urlBase+url."""
        return self.httpRequest(HttpConvention.kMethodDelete, url, data, params, headers, checkNotifications, **kwArgs)

    def httpGetRaw(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP GET and returns the Requests library result object."""
        kwArgs.setdefault('allow_redirects', True)
        return self.httpRequest(HttpConvention.kMethodGet, url, data, params, headers, checkNotifications, **kwArgs)

    def getWebObjectFromReply(self, reply, url, *urlExts):
        result = None
        if reply.text:
            try:
                result = WebObject(reply.json())
                if result is not None:
                    result._setSource_(WebObjectLocation(self, url, *urlExts))
            except:
                # allow for rare case of non-json response (e.g. /api/doc)
                result = reply.text
        return result

    def httpGet(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP GET and returns a WebObject representing the returned JSON payload."""
        reply = self.httpGetRaw(url, data, params, headers, checkNotifications, **kwArgs)
        return self.getWebObjectFromReply(reply, url)

    def httpHead(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP HEAD command and returns a Requests library result object.

        Parameters are the same as for HttpConvention.request().
        """
        return self.httpRequest(HttpConvention.kMethodHead, url, data, params, headers, checkNotifications, **kwArgs)

    def httpOptions(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP OPTIONS command and returns a Requests library result object.

        Parameters are the same as for HttpConvention.request().
        """
        kwArgs.setdefault('allow_redirects', True)
        return self.httpRequest(HttpConvention.kMethodOptions, url, data, params, headers, checkNotifications, **kwArgs)

    def _httpPollAsyncOperation(self, reply):
        statusUrl = None
        lastMethod = self.kMethodPost
        while True:
            if not reply.text:
                raise WebException("Status not returned from query to %s" % reply.url, extra=self._getFormattedErrorNotifications())
            time.sleep(0.1) # avoid DOS attack
            status = WebObject(reply.json())
            if not statusUrl:
                statusUrl = status.url
            if status.progress < 100:
                reply = self.httpGetRaw(statusUrl, allow_redirects=False)
                lastMethod = self.kMethodGet
            else:
                if status.state.lower() != "success":
                    raise WebException("%s to '%s' returned error. State: '%s' Message: '%s'" \
                        % (lastMethod, reply.url, status.state, status.message), extra=self._getFormattedErrorNotifications())
                return status

    def _httpGetTextResult(self, originalUrl, resultUrl):
        """ Helper method to get either a web object or text from a result URL."""
        result = None
        reply = self.httpGetRaw(resultUrl)
        if reply.text:
            try:
                result = self.getWebObjectFromReply(reply, originalUrl)
            except ValueError:
                result = reply.text
        return result

    def _httpStreamBinaryResultToFile(self, resultUrl, filehandle):
        """ Helper method to stream binary data from the server to a file."""
        reply = self.httpGetRaw(resultUrl, stream=True)
        for chunk in reply.iter_content(chunk_size=self.kStandardStreamingChunkSize):
            filehandle.write(chunk)
        filehandle.flush()

    def httpPostRaw(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP OPTIONS command and returns a Requests library result object.

        Parameters are the same as for HttpConvention.request().
        """
        return self.httpRequest(HttpConvention.kMethodPost, url, data, params, headers, checkNotifications, **kwArgs)

    def httpPost(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP OPTIONS command and returns a WebObject representing the returned JSON.

        If the URL represents an asyncronous operation (as indicated by a 202 response code), 
        then this method will block until the method completes.

        Parameters are the same as for HttpConvention.request().
        @return A WebObject representing the returned JSON or None if no JSON was returned
        """
        result = None
        reply = self.httpPostRaw(url, data, params, headers, checkNotifications, **kwArgs)
        if reply.status_code == httplib.ACCEPTED:
            status = self._httpPollAsyncOperation(reply)
            # if the service produces a result, return it (Otherwise just return None)
            if hasattr(status, "resultUrl"):
                result = self._httpGetTextResult(url, status.resultUrl)
        elif reply.text:
            # Use the json from the reply, but get the new object's real location from the header
            # If there is no location header, then we don't know where the object was created, 
            # so source we be set as None (so the object won't support httpPut, httpPatch or httpRefresh)
            result = self.getWebObjectFromReply(reply, reply.headers.get("location"))
        else:
            # Get the object using the location header
            location = reply.headers.get("location")
            result = location and self.httpGet(location) or None
        return result

    def httpPut(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP PUT command and returns a Requests library result object.

        Parameters are the same as for HttpConvention.request().
        """
        return self.httpRequest(HttpConvention.kMethodPut, url, data, params, headers, checkNotifications, **kwArgs)

    def httpPatch(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP PATCH command and returns a Requests library result object.

        Parameters are the same as for HttpConvention.request().
        """
        return self.httpRequest(HttpConvention.kMethodPatch, url, data, params, headers, checkNotifications, **kwArgs)

    def httpTrace(self, url="", data="", params={}, headers={}, checkNotifications=True, **kwArgs):
        """Performs an HTTP TRACE command and returns a Requests library result object.

        Parameters are the same as for HttpConvention.request().
        """
        return self.httpRequest(HttpConvention.kMethodTrace, url, data, params, headers, checkNotifications, **kwArgs)

    @classmethod
    def urljoin(cls, base, end):
        """ Join two URLs. If the second URL is absolute, the base is ignored.
        
        Use this instead of urlparse.urljoin directly so that we can customize its behavior if necessary.
        Currently differs in that it 
            1. appends a / to base if not present.
            2. casts end to a str as a convenience
        """
        Validators.checkNotNone(base, "base")
        Validators.checkNotNone(end, "end")
        if base and not base.endswith("/"):
            base = base + "/"
        return urljoin(base, str(end))


#-------------------------------------------------------------------------------------------
#
#   Session Services
#
#-------------------------------------------------------------------------------------------


class SessionsData(WebObjectBase):
    """A DAO to represent the parameter(s) necessary to create a session."""
    def __init__(self, sessionType):
        super(SessionsData, self).__init__()
        self.applicationType = sessionType

        
class SessionState(object):
    """Constants that represent possible values of a Session's state property."""
    kInitial = "Initial"
    kStarting = "Starting"
    kActive = "Active"
    kStopping = "Stopping"
    kStopped = "Stopped"
    kDead = "Dead"


class SessionSubState(object):
    """ These are not valid for all labs and states. """
    kUnconfigured = "Not Configured"
    kConfiguring = "Configuring"
    kConfigured = "Configured"
    kStarting = "Starting"
    kRunning = "Running"
    kStopping = "Stopping"

class TestState(object):
    """The states of a test run."""
    kNotStarted = "NotStarted"
    kStarting = "Starting"
    kRunning = "Running"
    kStopping = "Stopping"
    kStopped = "Stopped"

class Session(HttpConvention):
    """
        A class that represents a test session on the web server.
        A test session includes a configuration which can be executed via startTest
    """
    kOperationStopSession = "operations/stop"
    kOperationStartSession = "operations/start"
    kOperationGetNotificationsFormat = "notifications/sessions/%s"
    kOperationLoadConfigFormat = "config/%s/operations/load"
    kOperationSaveConfigFormat = "config/%s/operations/save"
    kOperationStartTestFormat = "testruns/%d/operations/start"
    kOperationStopTestFormat = "testruns/%s/operations/stop" 
    kOperationTestRunFormat = "testruns/%s" 

    kSessionBase = "sessions"
    _kJoin = "INTERNAL_JOIN"
    def __init__(self, connection, sessionType, sessionId=None, **kwArgs):
        """
            Create a session on the specified connection. Normally done using Connection.createSession
            Use join() to connect to an existing session

            @type connection: Connection
            @param connection: The connection on which to reference or establish a session
            @type sessionType: SessionType constant
            @param sessionType: The type of session to create.
        """
        Validators.checkNotNone(connection, "connection")
        Validators.checkSessionType(sessionType)
        super(Session, self).__init__(Session.kSessionBase, connection, **kwArgs)
        if sessionType == self._kJoin:
            # join the session
            # invoke this code through Session.join()
            Validators.checkInt(sessionId, "sessionId")
            # note that we can't check notifications until the session is up
            self._session = self.httpGet(str(sessionId), checkNotifications=False)
        else:
            # create the session
            # note that we can't check notifications until the session is up
            self._session = self.httpPost(data=SessionsData(sessionType), checkNotifications=False)
            sessionId = self.sessionId
        self.url = HttpConvention.urljoin(self.url, sessionId)
        self.currentTestRun = None

    @classmethod
    def join(cls, connection, sessionId, **kwArgs):
        """
            Attach to the specified session.

            @type connection: Connection object
            @param connection: the connection (server) on which to join
            @type sessionId: num
            @param sessionId: The session number to join
            @rtype Session: The joined session
        """
        Validators.checkNotNone(connection, "connection")
        Validators.checkInt(sessionId, "sessionId")
        return cls(connection, cls._kJoin, sessionId, **kwArgs)

    @property
    def sessionId(self):
        return self._session.id

    @property
    def sessionType(self):
        return self._session.applicationType

    @property
    def creationDate(self):
        return self._session.creationDate

    @property
    def creationTime(self):
        return self._session.creationTime

    @property
    def startingTime(self):
        return self._session.startingTime

    @property
    def stoppingTime(self):
        return self._session.stoppingTime

    @property
    def elapsedTime(self):
        return self._session.elapsedTime

    @property
    def state(self):
        return self._session.state

    @property
    def subState(self):
        return self._session.subState

    @property
    def testIsRunning(self):
        testRun = self.getCurrentTestRun()
        expectedStates = [TestState.kRunning, TestState.kStarting, TestState.kStopping]
        return testRun is not None and checkForPropertyValue(testRun, "testState", expectedStates, refresh=True)

    @property
    def testConfigName(self):
        return self._session.testConfigName

    def httpRefresh(self):
        self._session = self.httpGet()

    def runTest(self, trace=False):
        """Runs a test using the current configuration.

        Blocks until the test is done. On success returns an object with
        a testId property set to the id of the test. That id can be passed
        to the httpGetStatsCsvToFile API.

        @return test result
        @exception WebException
        """
        result = self.startTest(trace=trace)
        self.waitTestStopped(trace=trace)
        return result

    def startTest(self, trace=False):
        """Start the currently configured test, and returns immediately.

        On success returns an object with a testId property set to the id of the test. 
        That id can be passed to the httpGetStatsCsvToFile API.

        @return testRun
        @exception WebException
        """
        self.httpRefresh()
        if self.testIsRunning:
            raise WebException("Cannot startTest. Test already running")
        self.currentTestRun = self.httpPost("testruns")
        self.httpPost(self.kOperationStartTestFormat % self.currentTestRun.testId)
        return self.currentTestRun

    def stopTest(self, testId=None, graceful=False, trace=False):
        """Stop the currently running test."""
        self.httpPost(self.kOperationStopTestFormat % self.currentTestRun.testId, WebObject(gracefulStop=graceful))
        self.waitTestStopped(testId=testId, trace=trace)

    def getCurrentTestRun(self):
        """Get the WebObject representing the run of the last started test.

        The current test run is started by the last call to startTest.
        WaitTestStopped will delete the current test run when it exits.
        @return the current test run
        """
        return self.currentTestRun

    def getTestRun(self, testId):
        """Returns the testRun WebObject for the specified id.

        @param testId: The id of the testRun requested
        @return a testRun WebObject
        """
        return self.httpGet(self.kOperationTestRunFormat % testId)

    def waitTestStopped(self, testId=None, timeout=None, trace=False):
        """Waits until the currently running test stops.

        @param testId: the id of the test to wait for (e.g. from startTest().testId)
        @param timeout: max number of seconds to wait.
        @param trace: true to print polled value"""
        if testId is not None:
            testRun = self.getTestRun(testId)
        else:
            testRun = self.getCurrentTestRun()
            if not testRun:
                raise ValueError("Either testId must be specified, or a current test must have been created using startTest().")
        waitForProperty(testRun, "testState", [TestState.kStopped], timeout=timeout, trace=trace)
        self.checkNotifications()
        self.currentTestRun = None

    def getErrorNotifications(self):
        """Returns only error notifications, if any are in the queue."""
        return [notification for notification in self.getNotifications() if notification.level == "Error"]

    def checkNotifications(self):
        """Checks the notifications for errors and raises a WebException with the errors if any are found."""
        notifications = self.getErrorNotifications()
        if notifications:
            notificationMsgs = [notification.message for notification in notifications]
            raise WebException("The test failed with the following error(s): %s" % notificationMsgs)

    def startSession(self):
        """Start the session. The session must be started before being used."""
        # The session currently automatically starts on creation, but will not do so in the future
        # For now, for this synchronous call, we just wait for it to finish starting.
        self.httpPost(self.kOperationStartSession)
        self._waitForProperty("state", [SessionState.kActive], validValues=[SessionState.kInitial, SessionState.kStarting])

    def stopSession(self):
        """Bring down this session.

        The session was created by calling the Session constructor, and started using the startSession API
        """
        self.httpPost(self.kOperationStopSession)
        self._waitForProperty("state", [SessionState.kStopped], validValues=[SessionState.kActive, SessionState.kStopping])

    def getNotifications(self):
        """Returns a possibly-empty list of currently posted notifications for this session."""
        # session-specific, but resides under api/notifications/sessions/{id}
        return self.parentConvention.httpGet(self.kOperationGetNotificationsFormat % self.sessionId, checkNotifications=False)

    def saveConfiguration(self, configName, description="", overwrite=False):
        """Save the current configuration to the specified configuration name.

        @type configName: string
        @param configName: the configuration name
        """
        Validators.checkConfigName(configName)
        self.httpPost(self.kOperationSaveConfigFormat % self.sessionType, WebObject(name=configName, description=description, overwrite=overwrite))

    def loadConfiguration(self, configName, description=""):
        """Replace the current configuration with the configuration from the specified configuration name.

        @type configName: string
        @param configName: the configuration name
        """
        Validators.checkConfigName(configName)
        self.httpPost(self.kOperationLoadConfigFormat % self.sessionType, WebObject(name=configName))

    def findConfigurationByName(self, configName, raiseException=True):
        """Returns an object describing the named configuration.

        Throws an exception if raiseException is True and no configuration is found.
        @return A WebObject describing the configuration, or None if the configuration is not found (and raiseException is False)
        """
        Validators.checkConfigName(configName)
        return self.parentConvention.findConfigurationByName(self.sessionType, configName, raiseException)

    def exportConfigurationToFile(self, configName, exportFile):
        """Export a configuration, identified by its name, to a file.

        @param configName: the name of the config to export
        @param exportFile: a file-like object to write the configuration to. Must be opened in binary mode.
        """
        config = self.findConfigurationByName(configName)
        self.parentConvention.exportConfigurationToFileById(self.sessionType, config.id, exportFile)

    def importConfigurationFromFile(self, importFile):
        """ imports a configuration from the specified file.

        The file must have been created by a previous export operation.
        @param importFile: a file-like object to read the configuration from
        """
        return self.parentConvention.importConfigurationFromFile(self.sessionType, importFile)

    def getConfigurations(self):
        """Return a list of available configurations.

        Not really session-specific, but available on session for convenience.
        """
        return self.parentConvention.getConfigurations(self.sessionType) or WebListProxy()

    def deleteConfiguration(self, configName):
        """Deletes a configuration, given its sessionType and name

        @param sessionType: a string with the type of session
        @param configName: the name of the configuration to delete
        """
        config = self.findConfigurationByName(configName)
        self.parentConvention.deleteConfigurationById(self.sessionType, config.id)

    def _waitForProperty(self, propertyName, targetValues, validValues=[], invalidValues=[], timeout=None, trace=False):
        """Wait for the session to enter any of the specified targetStates. 

        Caller can also specified a set of valid or invalid states 
        """
        waitForProperty(self, propertyName, targetValues, validValues, invalidValues, timeout, trace)

    def collectDiagnosticsToFile(self, diagFile, clientOnly=False):
        """Collects debug diagnostics for the session and downloads them to a file

        @param exportFile: a file-like object to send the configuration to. Must be opened in binary mode
        """
        Validators.checkFile(diagFile, "diagFile")
        reply = self.parentConvention.collectSessionDiagnostics(self.sessionId, clientOnly)
        if reply.status_code == httplib.ACCEPTED:
            status = self._httpPollAsyncOperation(reply)
            self._httpStreamBinaryResultToFile(status.resultUrl, diagFile)
        else:
            raise WebException("Unexpected status code from request to collect diagnostics: %s" % reply.status_code)
    
    
    def registerStatsRequest(self,  statsRequest):
        """Registers a stats request object on the server for the current test. The user can register multiple requests for the same test.
		
        @param statsRequest: the StatsRequest object to register on the server.

        @returns An object that can be used to query for data from the server
		@raises WebException
        """

        if not isinstance(statsRequest, StatsRequest):
            raise ValueError("The '%s' parameter is not a StatsRequest object. Was %s." % ("statsRequest", statsRequest))           

        reply = self.httpPostRaw("stats/registration?append=true", WebListProxy([statsRequest]))
        return StatsReader(self, statsRequest)

    def _unregisterStatsRequest(self, statsRequest):
        """Unregisters a stat request on the server.

        @param statsRequest: the StatsRequest object to unregister.

        """
        
        if not isinstance(statsRequest, StatsRequest):
            raise ValueError("The '%s' parameter is not a StatsRequest object. Was %s." % ("statsRequest", statsRequest))

        self.httpPostRaw("stats/deregistration", WebListProxy([statsRequest]))
        

    def _getRealtimeData(self, statsRequest, startTimestamp = 0):
        """ Gets the snapshots with recent time stamp than the one specified for the specified stats request object.

        @param statsRequest:   the stats request object to request data for
        @param startTimestamp: reference time stamp
        
        """
        
        if not isinstance(statsRequest, StatsRequest):
            raise ValueError("The '%s' parameter is not a StatsRequest object. Was %s." % ("statsRequest", statsRequest))

        Validators.checkLong(startTimestamp, "startTimestamp")
                
        try:
            reply = self.httpPost("stats/data/cache" , WebListProxy([statsRequest]), {'startTimestamp' : startTimestamp} )
        except WebException as e:
            raise StandardError("The server has thrown an exception, please check the input parameters \n %s" %e)
        
        if reply is None:
            return None
        else:
            result = json.loads(reply)["map"]
            if statsRequest.id in result:
                return result[statsRequest.id]
            else:
                return None


#-------------------------------------------------------------------------------------------
#
#    User Administration
#
#-------------------------------------------------------------------------------------------


class UserRole(object):
    kUser = "User"
    kAdmin = "Admin"
    kGuest = "Guest"


class UserPermission(object):
    kAll = "*"
    kAllApps = "apps:*"

    @classmethod
    def kSpecificApp(cls, appName):
        """Used to get the constant representing permission to use the specified appName.

        @param appName: A legal application name available on the server.
        """
        return "apps:%s" % appName


class UserAdmin(HttpConvention):
    """A class with User Administration methods. Get an instance using getUserAdmin on a Connection."""

    kUserAdminBase = "auth"
    kRelUsersUrl = "users"
    kRelUserFormat = "users/%s"

    def __init__(self, connection, **kwArgs):
        """
            Create a session on the specified connection. Normally done using Connection.getUserAdmin
            Use join() to connect to an existing session

            @type connection: Connection
            @param connection: The connection on which to reference or establish a session
            @type sessionType: SessionType constant
            @param sessionType: The type of session to create.
        """
        Validators.checkNotNone(connection, "connection")
        super(UserAdmin, self).__init__(self.kUserAdminBase, connection, **kwArgs)

    def getUsers(self):
        """Returns a list of users registered with the connected-to server."""
        return self.httpGet(self.kRelUsersUrl)

    def findUser(self, username=None):
        """Returns an object describing the specified user, or throws an exception if not found.

        @param username: the login name of the user to find. Defaults to the current user
        """
        if username:
            Validators.checkString(username, "username")
        # avoid permission error on server for non-Admins working on their own account
        # (getUsers fails for non-Admins)
        currentUser = self.getCurrentUser()
        if not username or username == currentUser:
            return currentUser
        else:
            for user in self.getUsers():
                if user.username == username:
                    return user
        raise WebException("No such user: %s" % username)

    def getCurrentUser(self):
        """Returns an object describing the current user.

        The object will be the same as the individual list elements returned by findUser.
        """
        # get the current user info from the auth session (api/auth/session is different than api/sessions)
        userInfo = self.httpGet("session")
        # web service doensn't provide id directly, but it is last element of userAccountUrl
        userId = userInfo.userAccountUrl.split("/")[-1]
        # auth/session doesn't provide full user info, so we need to make another round trip
        return self.httpGet(self.kRelUserFormat % userId)

    def createUser(self, 
                   username,
                   password,
                   fullname="", 
                   email="", 
                   roles=[UserRole.kUser], 
                   permissions=[UserPermission.kAllApps]):
        """Creates a user with the specified username, password, and other properties.

        @param username: the login name of the user
        @param password: the password for the username
        @param email: the email address of the user
        @param fullname: the real name of the user
        @param roles: A list of UserRole constants that reflect the users use of the web site
        @param permissions: A list of UserPermission constants that reflect the permissions of the user for the web site
        """
        Validators.checkNonEmptyString(username, "username")
        Validators.checkNonEmptyString(password, "password")
        Validators.checkString(email, "email")
        Validators.checkString(fullname, "fullname")
        return self.httpPost(self.kRelUsersUrl, WebObject(username=username, 
                                                          password=password, 
                                                          email=email, 
                                                          fullname=fullname, 
                                                          roles=roles, 
                                                          permissions=permissions))

    def deleteUser(self, username):
        """Deletes the specified user.

        @param username: the login name of the user
        """
        user = self.findUser(username)
        self.httpDelete(self.kRelUserFormat % user.id)

    def changePassword(self, username, password, oldpassword=None):
        """Changes the password for a given user.

        @param username: the login name of the user
        @param password: the new password for the username
        @param oldpassword: the old password for the user. Only required for non-admins
        """
        if oldpassword:
            Validators.checkString(oldpassword, "oldpassword")
        Validators.checkNonEmptyString(password, "password")
        user = self.findUser(username)
        user._unlock_() # allow setting of undefined properties
        user.password = password
        if oldpassword:
            # don't even send it unless its provided
            user.oldpassword = oldpassword
        self.httpPut(self.kRelUserFormat % user.id, user)

    def setEmail(self, username, email):
        """Sets a new email address for a given user.

        @param username: the login name of the user
        @param email: the new email for the username
        """
        Validators.checkString(email, "email")
        user = self.findUser(username)
        user.email = email
        self.httpPut(self.kRelUserFormat % user.id, user)
    
    def setFullname(self, username, fullname):
        """Change a the fullName property of a user.

        There is no special validation of fullName. It is just a string for the admin's convenience
        @param username: the login name of the user
        @param fullname: the new full user name
        """
        Validators.checkString(fullname, "fullname")
        user = self.findUser(username)
        user.fullname = fullname
        self.httpPut(self.kRelUserFormat % user.id, user)

    def getAvailableRoles(self):
        """Returns a list of known Roles."""
        return self.httpGet("roles")

    def setRoles(self, username, roles):
        """Assigns a new list of roles (UserRole constants) to a user.

        @param username: the login name of the user
        @param roles: the list set of roles for the user
        """
        Validators.checkList(roles, "roles")
        user = self.findUser(username)
        user.roles = roles
        self.httpPut(self.kRelUserFormat % user.id, user)

    def getAvailablePermissions(self):
        """Returns a list of known permissions."""
        return self.httpGet("permissions")

    def setPermissions(self, username, permissions):
        """Assigns a new list of permissions (UserPermission constants) to a user.

        @param username: the login name of the user
        @param roles: the new list of roles for the user
        """
        Validators.checkList(permissions, "permissions")
        user = self.findUser(username)
        user.permissions = permissions
        self.httpPut(self.kRelUserFormat % user.id, user)


#-------------------------------------------------------------------------------------------
#
#    Connection to Server
#
#-------------------------------------------------------------------------------------------

class Connection(HttpConvention):

    kOperationGetConfigurationsFormat = "configurations/%s"
    kOperationDeleteConfigFormat = "configurations/%s/%s"
    kOperationImportConfigFormat = "configurations/%s/import"
    kOperationExportConfigFormat = "configurations/%s/%s/export"
    kOperationCollectSessionDiags = "diagnostics/sessions/%s/diags"
    kHeaderContentType = "content-type"
    kHeaderApiKey = "X-Api-Key"
    kHeaderReferrers = "referers"
    kContentJson = "application/json"
    kImportFormElement = "fileId"

    """ A class that represents a connection to an Ixia web app server and managing sessions there-on """
    def __init__(self, siteUrl, apiVersion, userkey="", username="", password="", params={}, headers={}, clsSession=Session, **kwArgs):
        """
            Construct a Connection instance to use for accessing an Ixia web app server

            Connects to the site specified by siteUrl with the specified apiVersion (currently only "v1" is supported).
            Either userkey or both username and password must be passed in. If username and password are supplied, then
            getuserkey may be called to determine the corresponding user key for later use.

            @type  siteUrl: string
            @param siteUrl: the top URL serving an Ixia web app (may require "IpAddress:portNum")            
            @type  userkey: string
            @param userkey: the key for the registered user running the script.
            @type  username: string
            @param username: this and password may be specified instead of userkey
            @type  password: string
            @param password: this and username may be specified instead of userkey
            @type  params: dictionary 
            @kwarg params: URL parameters to always use for this connection
            @type  headers: dictionary 
            @kwarg headers: HTTP headers to always use for this connection
            @type  kwArgs: dictionary
            @kwarg kwArgs: additional keyword args (future expansion)
            @except Throws a WebException if the connection was not successful
        """
        Validators.checkNonEmptyString(siteUrl, "siteUrl")
        Validators.checkNonEmptyString(apiVersion, "apiVersion")
        # initialize the user key. If not provided, it will be reinitialized from server in _getOrFetchuserkey
        if userkey:
            Validators.checkString(userkey, "userkey")
        self._userkey = userkey
        # default content type is json
        headers.setdefault(self.kHeaderContentType, self.kContentJson)
        super(Connection, self).__init__(HttpConvention.urljoin(siteUrl, "api"), params=params, headers=headers, **kwArgs)
        # we had to initialize our connection first in case we have to fetch user key from server here
        self.checkApiVersion(apiVersion)
        self.url = HttpConvention.urljoin(self.url, apiVersion)
        self.updateHeaders({self.kHeaderApiKey: self._getOrFetchuserkey(username, password)})
        self.checkScriptApiVersion()
        # try one URL that requires authentication to be sure we're connected
        self.httpGet("auth/ping")
        # initialize the session class which will be used to create sessions
        self._clsSession = clsSession

    def _getOrFetchuserkey(self, username=None, password=None):
        # internal method to get the user key. If user key not alreay set, then username and password must be supplied.
        if not self._userkey:
            Validators.checkNonEmptyString(username, "username")
            Validators.checkNonEmptyString(password, "password")
            # we use a session object here because it tracks cookies. Maybe we'll use it for HttpConvention someday.
            httpSession = requests.Session()
            httpSession.headers.update(self.headers);
            sessionKeyUrl = self.resolveUrl("auth/session")
            reply = httpSession.post(sessionKeyUrl, str(WebObject({"username": username, "password": password})), verify=False)
            if not reply.ok and reply.status_code == httplib.UNAUTHORIZED:
                raise WebException("Invalid username and/or password.")
            self.check(reply, self.kMethodPost, sessionKeyUrl, checkNotifications=False)
            try:
                keyUrl = self.resolveUrl("auth/session/key")
                reply = httpSession.get(keyUrl)
                self.check(reply, self.kMethodGet, keyUrl, checkNotifications=False)
                if not reply.text:
                    raise WebException("Authorization response not understood")
                response = WebObject(reply.json())
                self._userkey = response.apiKey
            finally:
                # log out of the session
                reply = httpSession.delete(sessionKeyUrl)
                self.check(reply, self.kMethodPost, sessionKeyUrl, checkNotifications=False)
        return self._userkey

    def getUserKey(self):
        """Return the user key, either supplied or determined from the username+password."""
        return self._userkey

    def checkApiVersion(self, apiVersion):
        Validators.checkNotNone(apiVersion, "apiVersion")
        availableVersions = [info.version for info in self.httpGet("versions")]
        if apiVersion not in availableVersions:
            raise WebException("API version %s not in available versions: %s" % (apiVersion, availableVersions))

    def checkScriptApiVersion(self):
        versionInfoList = self.httpGet("scriptapi/versions")
        versions = [versionInfo.version for versionInfo in versionInfoList]
        for version in kSupportedScriptApiVersions:
            if version in versions:
                break;
        else:
            raise WebException("None of the script api versions supported by this library %s are supported by the server which supports only %s. Please use a compatible webapi library." \
                                % (kSupportedScriptApiVersions, versions))

    def getSessionTypes(self):
        """ Returns a list of available session types."""
        sessionTypes = self.httpGet("applicationtypes")
        return [session.type for session in sessionTypes]

    def createSession(self, sessionType):
        """ Creates a new session.

        @param sessionType: The type of session to create, e.g. storagelab, contactcenter
        """
        Validators.checkSessionType(sessionType)
        return self._clsSession(self, sessionType)

    def joinSession(self, sessionId):
        """ Joins a previously created session.

        Note that the session could have been created by a different script or user

        @param sessionId: the session number to join
        """
        Validators.checkInt(sessionId, "sessionId")
        return self._clsSession.join(self, sessionId)

    def startSession(self, sessionId):
        """Start the specified session.

        Normally this is just done using session.startSession(), but is provided
        for completeness.

        @param sessionId: the session number to start        
        """
        return self.joinSession(sessionId).startSession()

    def stopSession(self, sessionId):
        """Stop the specified session.

        Can also be done using session.stopSession. This API on the connection is
        useful for scripts that didn't start the session

        @param sessionId: the session number to stop
        """
        return self.joinSession(sessionId).stopSession()

    def getSessions(self):
        """Return a list of session IDs."""
        return [session.id for session in self.httpGet("sessions")]

    def getConfigurations(self, sessionType):
        """Return a list of available configurations.

        Configurations may only be loaded into or saved from an active session
        """
        Validators.checkSessionType(sessionType)
        return self.httpGet(self.kOperationGetConfigurationsFormat % sessionType)

    def findConfigurationByName(self, sessionType, configName, raiseException=True):
        """Finds a specific configuration by sessionType and name
        """
        Validators.checkSessionType(sessionType)
        Validators.checkConfigName(configName)
        result = None
        matches = filter(lambda x: x.name == configName, self.getConfigurations(sessionType))
        if matches:
            result = matches[0]
        elif raiseException:
            raise WebException("No such %s configuration: '%s'." % (sessionType, configName))
        return result

    def exportConfigurationToFileById(self, sessionType, configId, exportFile):
        """Export a configuration of a specific sessionType and identified by its id to a file.

        @param sessionType: a string with the type of session.
        @param configId: the (numeric) id of the config to export.
        @param exportFile: a file-like object to write the configuration to. Must be opened in binary mode.
        """
        Validators.checkSessionType(sessionType)
        Validators.checkInt(configId, "configId")
        Validators.checkFile(exportFile, "exportFile")
        reply = self.httpGetRaw(self.kOperationExportConfigFormat % (sessionType, configId))
        exportFile.write(reply.content)

    def collectSessionDiagnostics(self, sessionId, clientOnly=False):
        Validators.checkInt(sessionId, "sessionId")
        absUrl = self.resolveUrl(self.kOperationCollectSessionDiags % sessionId)
        return self.httpPostRaw(absUrl, WebObject(clientOnly=clientOnly), stream=True)

    def importConfigurationFromFile(self, sessionType, importFile):
        """ Import a configration from a client-side file to the server.

        @param sessionType: a string with the type of session
        @param importFile: a file-like object to read the configuration from
        @return a WebObject if the server returns json. Otherwise, just the text value
        """
        Validators.checkSessionType(sessionType)
        Validators.checkFile(importFile, "importFile")
        #requests library does most of the work, but we have to customize the headers
        files={self.kImportFormElement: importFile}
        headers = self.resolveHeaders()
        # for the upload, the content type is not the default json payload
        del headers[self.kHeaderContentType]
        absUrl = self.resolveUrl(self.kOperationImportConfigFormat % sessionType)
        method = self.kMethodPost
        # we also omit the params and extras (that httpRequest would send) as these are not likely to ever be used here
        # but set verify to False to turn off SSL certificate validation
        reply = requests.request(method, absUrl, files=files, headers=headers, cookies=self.cookies, verify=False)
        self.check(reply, method, absUrl, checkNotifications=True)
        return self.getWebObjectFromReply(reply, absUrl)

    def deleteConfigurationById(self, sessionType, configId):
        """Deletes a configuration on the server, given its sessionType and id

        @param sessionType: a string with the type of session
        @param configId: the id of the configuration to delete
        """
        Validators.checkSessionType(sessionType)
        Validators.checkInt(configId, "configId")
        self.httpDelete(self.kOperationDeleteConfigFormat % (sessionType, configId), WebObject(applicationType=sessionType))

    def getAvailableStats(self, testOrResultId):
        """Retrieves a WebObject describing the set of available stat groups, stats and filters.

        @param testOrResultId: the test Id. Typically obtained by using the id member of the WebObject returned by runTest.
        """
        Validators.checkInt(testOrResultId, "testOrResultId")
        return self.httpGet("results/%s/schema" % testOrResultId)

    def getStatsCsvZipToFile(self, testOrResultId, statFile):
        """Retrieves the entire set of stats from the web server and writes them into the file-like object statFile.

        @param testOrResultId: the test Id. Typically obtained by using the id member of the WebObject returned by runTest.
        @param statFile: a file handle or file-like object to be written to with the CSV-formatted statistics data
        """
        Validators.checkInt(testOrResultId, "testOrResultId")
        Validators.checkFile(statFile, "statFile")
        reply = self.httpPostRaw("results/%s/zip" % testOrResultId, stream=True)
        if reply.status_code == httplib.ACCEPTED:
            status = self._httpPollAsyncOperation(reply)
            self._httpStreamBinaryResultToFile(status.resultUrl, statFile)
        else:
            raise WebException("Unable to retrieve csv for test/result %s" % testOrResultId)
        
    def getStatsCsvToFile(self, testOrResultId, statsCsvRequest, statFile):
        """Retrieves a specified set of stats from the web server and writes them into the file-like object statFile.

        @param testOrResultId: the test Id. Typically obtained by using the id member of the WebObject returned by runTest.
        @param statsCsvRequest: a StatsCsvRequest object specifying the set of stats to return in the CSV file.
        @param statFile: a file handle or file-like object to be written to with the CSV-formatted statistics data
        """
        Validators.checkInt(testOrResultId, "testOrResultId")
        Validators.checkNotNone(statsCsvRequest, "statsCsvRequest")
        Validators.checkFile(statFile, "statFile")
        reply = self.httpPostRaw("results/%s/csv" % testOrResultId, statsCsvRequest, stream=True)
        if reply.status_code == httplib.ACCEPTED:
            status = self._httpPollAsyncOperation(reply)
            self._httpStreamBinaryResultToFile(status.resultUrl, statFile)
        else:
            raise WebException("Unable to retrieve csv for test/result %s using request %s" % (testOrResultId, statsCsvRequest))
        statFile.flush()

    def getUserAdmin(self):
        """ Returns a UserAdmin object that can be used to create/edit/delete users.

        Note that the username (or corresponding key) used to create the Connection object must have a
        role of kAdmin and permissions of kAll to list or modify the settings for other users.
        """
        return UserAdmin(self)

class webApi(object):
    """A class that represents a connection to an ixia app server"""

    """Either userkey or both username and password must be provided
    @param siteUrl: The URL of the top-level of the site, e.g. http://testServer:8080
    @param siteVersion: The version of the site, e.g. 'v1', 'v2', etc.
    @param userkey: The user key for the user under which this script will run
    @param username: The login name of the user under which this script will run
    """
    @classmethod
    def connect(cls, siteUrl, siteVersion, userkey=None, username=None, password=None):
        return Connection(siteUrl, siteVersion, userkey, username, password)

class StatAggregation(object):
    kNone = "none"
    kSum = "sum"
    kMin = "min"
    kMax = "max"
    kAverage = "average"
    kRate = "rate"
    kMaxRate = "maxrate"
    kMinRate = "minrate"
    kPositiveRate = "positiverate"
    kPositiveMaxRate = "positivemaxrate"
    kPositiveMinRate = "positiveminrate"

    @classmethod
    def allSupportedAggregations(cls):
        return [cls.kNone, cls.kSum, cls.kMin, cls.kMax, cls.kAverage, cls.kRate, cls.kMaxRate, cls.kMinRate, cls.kPositiveRate, cls.kPositiveMaxRate, cls.kPositiveMinRate]

class Stat(WebObjectProxy):
    """Describes a stat object

    @param definition:       The definition of the stat.
    @param aggregationType: (optional) Defines how to aggregate the statistics. Default is "none"

    """
    def __init__(self, definition, aggregationType = StatAggregation.kNone):

        Validators.checkNonEmptyString(definition, "definition")
        Validators.checkString(aggregationType, "aggregationType")

        if not aggregationType in StatAggregation.allSupportedAggregations():
            raise ValueError("The specified aggregation type '%s' is not supported." % aggregationType)

        super(Stat, self).__init__(**{ "group": string.split(definition, ':')[0], \
                                       "name": string.split(definition, ':')[-1], "aggregationType": aggregationType})

    def __getattr__(self, attribute):
        if "definition" == attribute:
            return self.name

    #Filtering region
    
    def Equals(self, rightItem):
        return StatFilter(self.definition, "=", rightItem)

    def NotEqual(self, rightItem):
        return StatFilter(self.definition, "!=", rightItem)

    def LessThan(self, rightItem):
        return StatFilter(self.definition, "<", rightItem)

    def LessOrEquals(self, rightItem):
        return StatFilter(self.definition, "<=", rightItem)

    def GreaterThan(self, rightItem):
        return StatFilter(self.definition, ">", rightItem)

    def GreaterOrEquals(self, rightItem):
        return StatFilter(self.definition, ">=", rightItem)

class StatKey(Stat):
    """ Represents a key that can uniquely identify returned rows
    """
    pass
    
class StatFilter(WebObjectProxy):
    """
        Describes a filter expression that can be used when registering a query to set conditions that a potential row must pass in order to be returned
    """

    def __init__(self, leftItem, operator, rightItem, type="arithmetic"):
        if isinstance(rightItem, basestring):
            if not rightItem:
                raise ValueError("The '%s' parameter can only be a number or a non empty string. Was %s." % ("rightItem", rightItem))
        elif not isinstance(rightItem, StatFilter):
            try:
                float(rightItem)
            except:
                raise ValueError("The '%s'parameter can only be a number or a non empty string. Was %s." % ("rightItem", rightItem))

        super(StatFilter, self).__init__(**{ "leftItem": leftItem, "operator": operator, "rightItem" : rightItem, "type" : type})
        
    def And(self, rightItem):
        return StatFilter(self, "and", rightItem, "boolean")
    
    def Or(self, rightItem):
        return StatFilter(self, "or", rightItem, "boolean")

class OrderByStat(WebObjectProxy):
    """Describes an OrderByStat object used to specify the stat used for ordering the results and the sort direction

    @param statDefinition: A Stat object, the definition of the stat.
    @param direction:      Specifies how the results will be ordered: ascending or descending

    """    

    def __init__(self, statDefinition, direction):        
        if isinstance(statDefinition, basestring):
            Validators.checkNonEmptyString(statDefinition, statDefinition)
            
            super(OrderByStat, self).__init__(**{ "definition": string.split(statDefinition, ':')[-1], \
                "ascending": direction, "aggregationType": StatAggregation.kNone })
        elif isinstance(statDefinition, Stat):
            super(OrderByStat, self).__init__(**{ "definition": statDefinition.definition, \
                "ascending": direction, "aggregationType": statDefinition.aggregationType })
        else:
            raise ValueError(Validators.kFormatRequiresType % ("statDefinition", \
                'a Stat object or stat definition', type(statDefinition), statDefinition))

        if not direction in [OrderDirection.kAsc, OrderDirection.kDesc]:
            raise ValueError("The specified direction for ordering '%s' is not supported." % direction)

class OrderDirection:
    kAsc = "true"
    kDesc = "false"

class _statsGroup(WebObjectProxy):
    """ Temporary class used to build the legacy DSO object, do not use it in your scripts

    @param name:    the name of the data source. Must match the name used in the XML definition of the stats schema
    @param stats:   a list of Stat objects.    

    """
    def __init__(self, name, stats=[], orderBy=[], filter = None):
        super(_statsGroup, self).__init__(**{ "name": name, "stats": stats, "orderBy": orderBy, "filter": filter})

class StatsRequest(WebObjectProxy):
    """Describes a stat query

    @param stats:     a list of Stat objects. The returned rows will have values for both.
    @param orderBy    (optional) the columns that should be used for ordering the rows of the result
    @param syncGroup: (optional) a string that identifies which synchronization group this query belongs to. Default is "all"
    @param limit:     (optional) limits the returned rows in each snapshot to the specified value. Default is 0 meaning that all rows will be returned.
    @param cacheSize: (optional) represents the number of snapshots that are kept on the server-side cache for this query. 
                        Default is 1 meaning that the server will store only the most recent snapshot.
    """

    def __init__(self, stats, orderBy=[], syncGroup="all", limit=0, cacheSize=1, filter = None):
        Validators.checkString(syncGroup, "syncGroup")
        Validators.checkInt(limit, "limit")
        Validators.checkInt(cacheSize, "cacheSize")
        
        Validators.checkList(orderBy, "orderBy")
        Validators.checkList(stats, "stats")

        # Iterate through a copy of the stats list to be able to modify the original one
        for stat in list(stats):
            if not (hasattr(stat, "name") and hasattr(stat, "aggregationType")):
                raise ValueError("The specified stat list has in invalid stat (%s) object of type %s" %(stat, type(stat)))

        groups = [_statsGroup(stats[0].group, stats, orderBy, filter)]
        super(StatsRequest, self).__init__(**{ "id":self.generateQueryId(), "groups": groups, \
                                               "syncGroup":syncGroup, "limit": limit, "cacheSize": cacheSize})

    def __getattr__(self, attribute):
        if "stats" == attribute:
            return self.groups[0].stats

        if "syncGroup" == attribute:
            return self.groups[0].name

    @staticmethod
    def generateQueryId():
        return "apiQuery_" + str(uuid.uuid4())
    @staticmethod
    def copy(statsRequest):
        """        
        Makes a copy of the received stats request object with a different id to allow the same request data to be registered multiple times in a session
        """
        theCopy = copy.deepcopy(statsRequest)
        theCopy.id = StatsRequest.generateQueryId()

        return theCopy

class StatsReader(object):
    kDefaultTimeout = 300

    def __init__(self, session, statsRequest):
        self.session = session
        self.statsRequest = statsRequest
        self._lastTimestamp = 0        
        self._sleepTime = 0.5 #sec
        self._snapshots = []
        self._currentSnapshotIndex = -1
        self.isClosed = False
        self.lock = threading.Lock()
        
    def getNextSnapshot(self, timeout=kDefaultTimeout):

        if self._snapshots and (self._currentSnapshotIndex + 1) < len(self._snapshots):
            self._currentSnapshotIndex += 1
        else:
            self._currentSnapshotIndex = 0

            # get all the avaialable snapshots from the server, we will return them one by one
            tries = 0
            
            while (tries * self._sleepTime < timeout and not self.isClosed):
                
                self.lock.acquire()
                try:
                    rawData = self.session._getRealtimeData(self.statsRequest, self._lastTimestamp)
                finally:
                    self.lock.release()
                
                if not rawData is None:
                    self._snapshots = [Snapshot(snapshotData, self.statsRequest) for snapshotData in rawData]

                if len(self._snapshots) > 0:
                    break
                
                time.sleep(self._sleepTime)
                tries+=1
            
            if (rawData is None or len(self._snapshots) == 0) and not self.isClosed:
                raise StatsTimeoutException("StatsReader.getNextData(): Timeout while trying to get values for queryId:" + self.statsRequest.id)
            if len(self._snapshots)>0 and not self.isClosed:
                self._lastTimestamp = self._snapshots[-1].timestamp;
        
        if not self.isClosed:
            return self._snapshots[self._currentSnapshotIndex]
        else:
            return None
        
    def close(self):
        self.isClosed = True
        
        self.lock.acquire()
        try:
            self.session._unregisterStatsRequest(self.statsRequest)
        finally:
            self.lock.release()
            
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

class StatsAsyncReader(object):

    """
    Wraps a StatsReader object and get snapshots asynchronously on the specified callback method.
    This approach to consume data is recommended for scenarios where data for multiple queries needs 
    to be consumed in parallel to prevent losing timestamps.
    By default data is consumed until the reader is closed, optionally a number of poll count limit 
    can be specified to limit the number of read snapshots until the reader is closed automatically
    """

    def __init__(self, statsReader, callback, pollCountLimit=0, timeout=StatsReader.kDefaultTimeout):
        self.statsReader = statsReader
        self.callback = callback
        self.timeout = timeout
        self.exception = None
        self.pollCountLimit = pollCountLimit
        self.currentPollCount = 0

        self.thread = threading.Thread(target = self._run)
        self.thread.start()        

    def _onExceptionCallback(self, exception):
        """
        Traps the exceptions on the worker thread and propagate them to the main thread
        """
        self.exception = exception

    def _run(self):
        lastSnapshot = None
        
        while True:
            try:
                currentSnapshot = self.statsReader.getNextSnapshot(timeout = self.timeout)
                if self.statsReader.isClosed:
                    break
                
                self.callback(self, currentSnapshot, lastSnapshot)
                lastSnapshot = currentSnapshot
                
                if (self.pollCountLimit > 0):
                    # Limit the number of read polls
                    self.currentPollCount += 1
                    if (self.currentPollCount == self.pollCountLimit) :
                        break
            except Exception, ex:
                self._onExceptionCallback(ex)
                raise

    def close(self):
        self.statsReader.close()
        self.thread.join()

    def __getattr__(self, attribute):
        if "isClosed" == attribute:
            return self.statsReader.isClosed
        
        if "isAlive" == attribute:
            return self.thread.isAlive()
        
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):        
        self.close()
   
class Snapshot(object):
    def __init__(self, rawData, statsRequest):
        self.rawData = rawData
        self.statsRequest = statsRequest
        self._columns = {}
        
        statIndex = 0;

        for col in statsRequest.stats:
            self._columns[col.definition] = statIndex
            statIndex += 1

        self.rows = [_Row(rowIndex, self.rawData["values"], self._columns) for rowIndex in range(0, len(self.rawData["values"]))]
        
    def __getattr__(self, attribute):
        if "timestamp" == attribute:
            return self.rawData["timestamp"]

    def getSummary(self):
        result = "Query Id:%s, Group: %s, TS:%s, %s rows" %(self.statsRequest.id, self.statsRequest.syncGroup, self.timestamp, len(self.rows))
        return result

    def printAsTable(self):
        result = "Query Id:%s, Group: %s" %(self.statsRequest.id, self.statsRequest.syncGroup)

        colWrap = [textwrap.wrap(self._buildColCaption(column),10) for column in self.statsRequest.stats]
        
        colHeader = ""
        lineIndex = 0;
        cont = 1
        while cont == 1:
            if lineIndex == 0:
                curLine = "%12s" % "Timestamp"
            else:
                curLine = "%12s" % ""
                
            cont = 0
            for col in colWrap:
                if lineIndex < len(col):
                    curLine += ("%12s" % col[lineIndex])
                    cont = 1
                else:
                    curLine += ("%12s" % "")

            colHeader += curLine
            if cont == 1:
                colHeader += "\n"
            lineIndex += 1
        
        result += "\n" + colHeader + "\n"
            
        for rowData in self.rawData["values"]:
            rowString = "%12s" % self.rawData["timestamp"]
            for cellValue in rowData:
                rowString += "%12s" % str(cellValue)[:11]
            result += rowString + "\n"

        return result

    def _buildColCaption(self, stat):
        return stat.definition

class _Row(object):
    def __init__(self, rowIndex, rawData, columns):
        self._rawData = rawData
        self._rowIndex = rowIndex
        self._columns = columns        

    def __getattr__(self, attribute):
        if "timestamp" == attribute:
            return self._rawData["timestamp"]

    def value(self, statName):
        return self._rawData[self._rowIndex][self._columns[statName]]
