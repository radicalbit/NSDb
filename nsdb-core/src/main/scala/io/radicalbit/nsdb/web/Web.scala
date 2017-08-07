package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.core.{Core, CoreActors}
import io.radicalbit.nsdb.api.Api

trait Web extends StaticResources with CoreActors with Core with Api