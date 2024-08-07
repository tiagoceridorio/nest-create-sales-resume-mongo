import { Injectable, Inject } from '@nestjs/common';
import { Db } from 'mongodb';

@Injectable()
export class AuditService {
  constructor(@Inject('DATABASE_CONNECTION') private database: Db) {}

  async updateAuditData() {
    try {
      const ordersCollection = this.database.collection('c_salesorder');
      const auditCollection = this.database.collection(
        'c_salesorder_integrationauditbydate',
      );

      const lastMonth = new Date();
      lastMonth.setDate(lastMonth.getDate() - 30);

      const pipeline = [
        {
          $match: {
            date: {
              $gte: lastMonth,
            },
          },
        },
        {
          $project: {
            date: {
              $dateToString: {
                format: '%Y-%m-%d',
                date: '$date',
              },
            },
            hasCubbo: {
              $cond: {
                if: { $ifNull: ['$cubbo', false] },
                then: true,
                else: false,
              },
            },
            hasAdminId: {
              $cond: {
                if: { $ifNull: ['$adminId', false] },
                then: true,
                else: false,
              },
            },
            isCompensated: {
              $not: {
                $in: [
                  '$status',
                  [
                    'pending',
                    'PAYMENT_REFUSED',
                    'PAYMENT_PENDING',
                    'PAYMENT_GENERATED',
                    'PAYMENT_FAILED',
                    'PAYMENT_EXPIRED',
                    'ORDER_DUPLICATED',
                  ],
                ],
              },
            },
          },
        },
        {
          $group: {
            _id: '$date',
            ordersWithCubbo: {
              $sum: {
                $cond: ['$hasCubbo', 1, 0],
              },
            },
            ordersWithoutCubbo: {
              $sum: {
                $cond: [
                  { $and: [{ $not: '$hasCubbo' }, '$isCompensated'] },
                  1,
                  0,
                ],
              },
            },
            ordersWithAdminId: {
              $sum: {
                $cond: ['$hasAdminId', 1, 0],
              },
            },
            compensatedOrders: {
              $sum: {
                $cond: ['$isCompensated', 1, 0],
              },
            },
            nonCompensatedOrders: {
              $sum: {
                $cond: ['$isCompensated', 0, 1],
              },
            },
          },
        },
        {
          $project: {
            _id: 0,
            date: '$_id',
            ordersWithCubbo: 1,
            ordersWithoutCubbo: 1,
            ordersWithAdminId: 1,
            compensatedOrders: 1,
            nonCompensatedOrders: 1,
          },
        },
        {
          $sort: { date: 1 },
        },
      ];

      const results = await ordersCollection.aggregate(pipeline).toArray();

      // Clear existing data
      await auditCollection.deleteMany({});

      // Insert new data
      if (results.length > 0) {
        await auditCollection.insertMany(results);
      }

      console.log('Audit data updated successfully.');
    } catch (error) {
      console.error('Error updating audit data:', error);
    }
  }

  async updateHourlyAuditData() {
    try {
      const ordersCollection = this.database.collection('c_salesorder');
      const hourlyAuditCollection = this.database.collection(
        'c_salesorder_integrationauditbyhour',
      );

      const today = new Date();
      today.setHours(0, 0, 0, 0);

      const pipeline = [
        {
          $match: {
            date: {
              $gte: today,
            },
          },
        },
        {
          $project: {
            hour: {
              $dateToString: {
                format: '%Y-%m-%d %H:00',
                date: '$date',
              },
            },
            hasCubbo: {
              $cond: {
                if: { $ifNull: ['$cubbo', false] },
                then: true,
                else: false,
              },
            },
            hasAdminId: {
              $cond: {
                if: { $ifNull: ['$adminId', false] },
                then: true,
                else: false,
              },
            },
            isCompensated: {
              $not: {
                $in: [
                  '$status',
                  [
                    'pending',
                    'PAYMENT_REFUSED',
                    'PAYMENT_PENDING',
                    'PAYMENT_GENERATED',
                    'PAYMENT_FAILED',
                    'PAYMENT_EXPIRED',
                    'ORDER_DUPLICATED',
                  ],
                ],
              },
            },
          },
        },
        {
          $group: {
            _id: '$hour',
            ordersWithCubbo: {
              $sum: {
                $cond: ['$hasCubbo', 1, 0],
              },
            },
            ordersWithoutCubbo: {
              $sum: {
                $cond: [
                  { $and: [{ $not: '$hasCubbo' }, '$isCompensated'] },
                  1,
                  0,
                ],
              },
            },
            ordersWithAdminId: {
              $sum: {
                $cond: ['$hasAdminId', 1, 0],
              },
            },
            compensatedOrders: {
              $sum: {
                $cond: ['$isCompensated', 1, 0],
              },
            },
            nonCompensatedOrders: {
              $sum: {
                $cond: ['$isCompensated', 0, 1],
              },
            },
          },
        },
        {
          $project: {
            _id: 0,
            hour: '$_id',
            ordersWithCubbo: 1,
            ordersWithoutCubbo: 1,
            ordersWithAdminId: 1,
            compensatedOrders: 1,
            nonCompensatedOrders: 1,
          },
        },
        {
          $sort: { hour: 1 },
        },
      ];

      const results = await ordersCollection.aggregate(pipeline).toArray();

      // Clear existing data
      await hourlyAuditCollection.deleteMany({});

      // Insert new data
      if (results.length > 0) {
        await hourlyAuditCollection.insertMany(results);
      }

      console.log('Hourly audit data updated successfully.');
    } catch (error) {
      console.error('Error updating hourly audit data:', error);
    }
  }
}
