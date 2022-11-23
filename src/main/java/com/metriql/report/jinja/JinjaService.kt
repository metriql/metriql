package com.metriql.report.jinja

import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.report.IAdHocService
import com.metriql.report.data.FilterValue
import com.metriql.report.jinja.JinjaApps.JinjaApp
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class JinjaService(val apps: List<JinjaApp>) : IAdHocService<ObjectNode> {

    override fun renderQuery(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: ObjectNode, reportFilters: FilterValue?): IAdHocService.RenderedQuery {
        TODO("not implemented")
    }


}
