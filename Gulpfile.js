
const gulp = require('gulp'),
    typescript = require('gulp-typescript'),
    merge = require('merge2'),
    tsProject = typescript.createProject('tsconfig.json');

gulp.task('compile', () => {
    const tsResult = gulp.src(['src/**/*.ts'])
        .pipe(tsProject());

    return merge([
        tsResult.dts.pipe(gulp.dest('dst')),
        tsResult.js.pipe(gulp.dest('dst')),
    ]);
});

gulp.task('watch', () => {
    return gulp.watch('src/**/*.ts', gulp.series('compile'));
});

gulp.task('default', gulp.series('compile', 'watch'));
