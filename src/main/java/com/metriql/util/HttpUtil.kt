package com.metriql.util

import io.netty.channel.ChannelFutureListener
import io.netty.channel.DefaultFileRegion
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.FullHttpResponse
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.LastHttpContent
import org.rakam.server.http.HttpServer
import org.rakam.server.http.RakamHttpRequest
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.RandomAccessFile
import java.io.UnsupportedEncodingException
import java.lang.Error
import java.net.URLDecoder
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.GregorianCalendar
import java.util.Locale
import java.util.TimeZone
import java.util.regex.Pattern
import javax.activation.MimetypesFileTypeMap

object HttpUtil {
    private const val HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz"
    private const val HTTP_DATE_GMT_TIMEZONE = "GMT"
    private const val HTTP_CACHE_SECONDS = 60 * 60 * 24
    private val INSECURE_URI = Pattern.compile(".*[<>&\"].*")
    private val mimeTypesMap = MimetypesFileTypeMap()

    @JvmStatic
    fun sendFile(request: RakamHttpRequest, file: File) {
        if (file.isHidden || !file.exists()) {
            sendError(request, HttpResponseStatus.NOT_FOUND)
            return
        }
        if (!file.isFile) {
            sendError(request, HttpResponseStatus.FORBIDDEN)
            return
        }

        // Cache Validation
        val ifModifiedSince = request.headers()[HttpHeaders.Names.IF_MODIFIED_SINCE]
        if (ifModifiedSince != null && ifModifiedSince.isNotEmpty()) {
            val dateFormatter = SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US)
            val ifModifiedSinceDate = try {
                dateFormatter.parse(ifModifiedSince)
            } catch (e: ParseException) {
                sendError(request, HttpResponseStatus.BAD_REQUEST)
                return
            }

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            val ifModifiedSinceDateSeconds = ifModifiedSinceDate.time / 1000
            val fileLastModifiedSeconds = file.lastModified() / 1000
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                sendNotModified(request, file)
                return
            }
        }
        val raf = try {
            RandomAccessFile(file, "r")
        } catch (ignore: FileNotFoundException) {
            sendError(request, HttpResponseStatus.NOT_FOUND)
            return
        }
        val fileLength = try {
            raf.length()
        } catch (e: IOException) {
            sendError(request, HttpResponseStatus.BAD_GATEWAY)
            return
        }

        val response: HttpResponse = DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        HttpHeaders.setContentLength(response, fileLength)
        setContentTypeHeader(response, file)
        setDateAndCacheHeaders(response, file)
        if (HttpHeaders.isKeepAlive(request)) {
            response.headers()[HttpHeaders.Names.CONNECTION] = HttpHeaders.Values.KEEP_ALIVE
        }
        val origin = request.headers()[HttpHeaders.Names.ORIGIN]
        if (origin != null && origin.isNotEmpty()) {
            response.headers()[HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN] = origin
        }
        request.context().write(response)
        val defaultFileRegion = DefaultFileRegion(raf.channel, 0, fileLength)
        request.context().write(defaultFileRegion, request.context().newProgressivePromise())
        val lastContentFuture = request.context().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
        if (!HttpHeaders.isKeepAlive(request)) {
            lastContentFuture.addListener(ChannelFutureListener.CLOSE)
        }
    }

    private fun setDateAndCacheHeaders(response: HttpResponse, fileToCache: File) {
        val dateFormatter = SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US)
        dateFormatter.timeZone = TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE)
        val time: Calendar = GregorianCalendar()
        response.headers()[HttpHeaders.Names.DATE] = dateFormatter.format(time.time)
        val mimeType: String = mimeTypesMap.getContentType(fileToCache)
        if (mimeType == "text/html") {
            response.headers()[HttpHeaders.Names.CACHE_CONTROL] = "no-cache"
        } else {
            response.headers()[HttpHeaders.Names.CACHE_CONTROL] = "private, max-age=" + HTTP_CACHE_SECONDS
        }
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS)
        response.headers()[HttpHeaders.Names.LAST_MODIFIED] = dateFormatter.format(Date(fileToCache.lastModified()))
    }

    @JvmStatic
    fun sendError(request: RakamHttpRequest, status: HttpResponseStatus) {
        HttpServer.returnError(request, status.reasonPhrase(), status)
    }

    fun sendNotModified(request: RakamHttpRequest, file: File) {
        val response: FullHttpResponse = DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_MODIFIED)
        setContentTypeHeader(response, file)
        val dateFormatter = SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US)
        dateFormatter.timeZone = TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE)
        val time: Calendar = GregorianCalendar()
        response.headers()[HttpHeaders.Names.DATE] = dateFormatter.format(time.time)
        request.response(response).end()
    }

    private fun setContentTypeHeader(response: HttpResponse, file: File) {
        response.headers()[HttpHeaders.Names.CONTENT_TYPE] = mimeTypesMap.getContentType(file.path)
    }

    @JvmStatic
    fun sanitizeUri(directory: File, uri: String, prefix: String = "/"): String {
        var uri = uri
        uri = try {
            URLDecoder.decode(uri, "UTF-8")
        } catch (e: UnsupportedEncodingException) {
            throw Error(e)
        }
        if (uri.isEmpty() || !uri.startsWith(prefix)) {
            throw MetriqlException(HttpResponseStatus.NOT_FOUND)
        }
        uri = uri.replace('/', File.separatorChar)

        // TODO: Simplistic dumb security check. Check for security issues
        return if (uri.contains(File.separator + '.') ||
            uri.contains('.'.toString() + File.separator) || uri[0] == '.' || uri[uri.length - 1] == '.' ||
            INSECURE_URI.matcher(uri).matches()
        ) {
            throw MetriqlException(HttpResponseStatus.NOT_FOUND)
        } else directory.path + File.separator + uri.substring(prefix.length)
    }
}
