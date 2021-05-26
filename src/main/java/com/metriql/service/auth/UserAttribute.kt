package com.metriql.service.auth

import com.metriql.db.JSONBSerializable
import com.metriql.util.PolymorphicTypeStr

data class UserAttribute(
    val type: UserAttributeDefinition.Type,
    @PolymorphicTypeStr<UserAttributeDefinition.Type>(externalProperty = "type", valuesEnum = UserAttributeDefinition.Type::class)
    val value: UserAttributeDefinition.Type.UserAttributeValue<*>?
)

@JSONBSerializable
typealias UserAttributeValues = Map<String, UserAttribute>

typealias UserAttributeFetcher = (ProjectAuth) -> UserAttributeValues
