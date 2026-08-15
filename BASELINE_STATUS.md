# Baseline Status

هذه حالة الحزمة قبل عمل المرشح:

- `npm ci`: ناجح.
- `npm run typecheck`: ناجح.
- `npm run test:smoke`: ناجح، 2 من 2.
- `npm test`: 9 اختبارات ناجحة و6 فاشلة عمدًا؛ حالات الفشل هي جزء المهمة وليست عطل إعداد.
- `migrations/001_baseline.sql`: تم تطبيقها وفحص مخططها بنجاح في PostgreSQL متوافق مضمّن.
- `npm run test:db`: يتطلب PostgreSQL 16 حقيقية عبر Docker؛ يجب تشغيله محليًا بعد `npm run db:up` و`npm run db:migrate`.

الهدف النهائي عند التسليم: نجاح `npm run verify` بالكامل دون حذف أو تعطيل اختبارات القبول.
