package com.metriql

import com.metriql.deployment.Deployment
import com.metriql.service.auth.ProjectAuth
import com.metriql.util.MetriqlException
import io.jsonwebtoken.Claims
import io.jsonwebtoken.JwsHeader
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import io.jsonwebtoken.SignatureException
import io.jsonwebtoken.SigningKeyResolver
import io.jsonwebtoken.UnsupportedJwtException
import io.netty.handler.codec.http.HttpHeaders.Names.AUTHORIZATION
import io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED
import io.trino.server.security.BasicAuthCredentials
import org.rakam.server.http.HttpServerBuilder.IRequestParameterFactory
import org.rakam.server.http.IRequestParameter
import org.rakam.server.http.RakamHttpRequest
import java.io.ByteArrayInputStream
import java.io.IOException
import java.lang.reflect.Method
import java.nio.charset.StandardCharsets
import java.security.Key
import java.security.cert.CertificateFactory
import java.time.ZoneId
import java.util.Base64
import javax.crypto.spec.SecretKeySpec
import javax.ws.rs.core.HttpHeaders

data class UserContext(val user: String?, val pass: String?, val request: RakamHttpRequest)

class MetriqlAuthRequestParameterFactory(
    private val oauthApiSecret: String?,
    private val deployment: Deployment,
    private val timezone: ZoneId?
) : IRequestParameterFactory {
    override fun create(m: Method): IRequestParameter<ProjectAuth> {
        return IRequestParameter<ProjectAuth> { _, request ->

            val token = request.headers().get(AUTHORIZATION)?.split(" ".toRegex(), 2)
            val auth = when (token?.get(0)?.lowercase()) {
                "bearer" -> {
                    val parser = Jwts.parser()
                    val secret = oauthApiSecret ?: throw getAuthException("Oauth")
                    val key = loadKeyFile(secret)
                    parser.setSigningKeyResolver(object : SigningKeyResolver {
                        override fun resolveSigningKey(header: JwsHeader<out JwsHeader<*>>?, claims: Claims?): Key? {
                            val algorithm = SignatureAlgorithm.forName(header!!.algorithm)
                            return key.getKey(algorithm)
                        }

                        override fun resolveSigningKey(header: JwsHeader<out JwsHeader<*>>?, plaintext: String?): Key? {
                            val algorithm = SignatureAlgorithm.forName(header!!.algorithm)
                            return key.getKey(algorithm)
                        }
                    })

                    try {
                        parser.parse(token[1]).body as Map<String, Any?>
                    } catch (e: Exception) {
                        null
                    }

                    ProjectAuth.singleProject(null)
                }
                "basic" -> {
                    val userPass = String(Base64.getDecoder().decode(token[1]), StandardCharsets.UTF_8).split(":".toRegex(), 2)
                    deployment.getAuth(UserContext(userPass[0], userPass[1], request))
                }
                else -> null
            }

            if (auth == null) {
                if (request.headers().get("sec-fetch-mode") == "navigate") {
                    val redirectUri = ""
                    val tokenUri = ""
                    request.addResponseHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer x_redirect_server=\"$redirectUri\", x_token_server=\"$tokenUri\"")
                    if (!deployment.isAnonymous()) {
                        request.addResponseHeader(HttpHeaders.WWW_AUTHENTICATE, BasicAuthCredentials.AUTHENTICATE_HEADER)
                    }
                }
                if (!deployment.isAnonymous()) {
                    throw MetriqlException(UNAUTHORIZED)
                } else {
                    deployment.getAuth(UserContext(null, null, request))
                }
            } else {
                auth.copy(source = request.headers().get("X-Metriql-Source"), timezone = timezone)
            }
        }
    }
}

private fun getAuthException(type: String): MetriqlException {
    return MetriqlException("$type is not supported in this environment", UNAUTHORIZED)
}

private fun loadKeyFile(value: String): LoadedKey {
    return try {
        val cf = CertificateFactory.getInstance("X.509")
        val cert = cf.generateCertificate(ByteArrayInputStream(value.toByteArray(StandardCharsets.UTF_8)))
        LoadedKey(cert.publicKey, null)
    } catch (var4: Exception) {
        try {
            val rawKey = Base64.getMimeDecoder().decode(value.toByteArray(StandardCharsets.US_ASCII))
            LoadedKey(null, rawKey)
        } catch (var3: IOException) {
            throw SignatureException("Unknown signing key id")
        }
    }
}

class LoadedKey(private val publicKey: Key?, private val hmacKey: ByteArray?) {
    fun getKey(algorithm: SignatureAlgorithm): Key? {
        return if (algorithm.isHmac) {
            if (hmacKey == null) {
                throw UnsupportedJwtException(String.format("JWT is signed with %s, but no HMAC key is configured", algorithm))
            } else {
                SecretKeySpec(hmacKey, algorithm.jcaName)
            }
        } else publicKey ?: throw UnsupportedJwtException(String.format("JWT is signed with %s, but no key is configured", algorithm))
    }
}
