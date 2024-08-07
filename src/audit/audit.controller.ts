import { Controller, Get } from '@nestjs/common';
import { AuditService } from './audit.service';
import { Cron, CronExpression } from '@nestjs/schedule';

@Controller('audit')
export class AuditController {
  constructor(private readonly auditService: AuditService) {}

  @Get('update-daily')
  @Cron(CronExpression.EVERY_HOUR)
  async updateDailyAudit() {
    await this.auditService.updateAuditData();
    return { message: 'Daily audit data updated successfully.' };
  }

  @Get('update-hourly')
  async updateHourlyAudit() {
    await this.auditService.updateHourlyAuditData();
    return { message: 'Hourly audit data updated successfully.' };
  }
}
