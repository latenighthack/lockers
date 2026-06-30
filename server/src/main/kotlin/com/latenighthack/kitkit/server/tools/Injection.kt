package com.latenighthack.kitkit.server.tools

import me.tatarka.inject.annotations.Scope

/** kotlin-inject scope for a single service module's object graph. */
@Scope
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER)
annotation class ServiceScope
