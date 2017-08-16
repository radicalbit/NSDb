package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.api.Api
import io.radicalbit.nsdb.core.{CoreActors, WebBootedCore}

object Rest extends App with WebBootedCore with CoreActors with Api with StaticResources